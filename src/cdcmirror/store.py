"""单一职责：本地 sqlite3 持久化。

提供两套互不干扰的能力：

* 基线 happy-path：幂等 ``upsert`` / 计数 / 全量读取（``records`` 表）；
* CDC 有序计划落库：``apply_unit`` 按排序单元做 upsert/delete，并通过
  ``processed_events`` / ``processed_txns`` / ``checkpoints`` 三张表支撑
  去重、事务级幂等与分片水位线检查点。

调用方（checkpoint 阶段）必须在同一个连接、同一个事务内写完数据与检查点再
``commit``：检查点提交前任何失败都整体回滚，保证原子性。
"""
import json
import sqlite3

from .config import DB_PATH


def connect():
    con = sqlite3.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS records(key TEXT PRIMARY KEY, data TEXT NOT NULL)")
    return con


def upsert(con, record):
    con.execute("INSERT INTO records(key,data) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                (record.key, json.dumps(record.data, ensure_ascii=False)))


def commit(con):
    con.commit()


def count(con):
    return con.execute("SELECT COUNT(*) FROM records").fetchone()[0]


def all_records(con):
    return [(k, json.loads(d)) for k, d in con.execute("SELECT key,data FROM records")]


# ---------------------------------------------------------------------------
# CDC 有序计划落库
# ---------------------------------------------------------------------------

def ensure_cdc_tables(con):
    """幂等建表：镜像行、已处理事件/事务、分片检查点、计划指纹。"""
    con.execute(
        "CREATE TABLE IF NOT EXISTS cdc_mirror("
        "key TEXT PRIMARY KEY, data TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS processed_events("
        "event_id TEXT PRIMARY KEY, txn_id TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS processed_txns("
        "txn_id TEXT PRIMARY KEY, commit_ts TEXT NOT NULL, fingerprint TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS checkpoints("
        "shard TEXT PRIMARY KEY, watermark INTEGER NOT NULL)"
    )


def mirror_key(event: dict) -> str:
    """镜像行稳定键：分片 + 表 + 主键（与事件 event_id 解耦）。"""
    return f"{event.get('shard', '')}:{event.get('table', '')}:{event.get('pk', '')}"


def apply_event(con, event: dict) -> str:
    """按排序后的单条事件落库，返回实际动作（delete/upsert/skip）。"""
    ensure_cdc_tables(con)
    eid = str(event["event_id"])
    if con.execute("SELECT 1 FROM processed_events WHERE event_id=?", (eid,)).fetchone():
        return "skip"
    key = mirror_key(event)
    op = event.get("op", "insert")
    if op == "delete":
        con.execute("DELETE FROM cdc_mirror WHERE key=?", (key,))
    else:  # insert / update / snapshot：同一稳定键幂等覆盖
        con.execute(
            "INSERT INTO cdc_mirror(key,data) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET data=excluded.data",
            (key, json.dumps(event, ensure_ascii=False, sort_keys=True)),
        )
    con.execute("INSERT OR IGNORE INTO processed_events(event_id,txn_id) VALUES(?,?)",
                (eid, str(event.get("txn_id", ""))))
    return op if op == "delete" else "upsert"


def apply_unit(con, unit: dict) -> dict:
    """应用一个已释放事务单元；整单元已处理时整体跳过（多次恢复幂等）。"""
    txn_id = unit["txn_id"]
    ensure_cdc_tables(con)
    if con.execute("SELECT 1 FROM processed_txns WHERE txn_id=?", (txn_id,)).fetchone():
        return {"txn_id": txn_id, "applied": 0, "skipped": len(unit["events"])}
    applied = 0
    actions = []
    for ev in unit["events"]:
        actions.append(apply_event(con, ev))
        applied += 1
    return {"txn_id": txn_id, "applied": applied,
            "skipped": 0, "actions": actions}


def save_checkpoint(con, shard: str, watermark: int) -> None:
    ensure_cdc_tables(con)
    con.execute(
        "INSERT INTO checkpoints(shard,watermark) VALUES(?,?) "
        "ON CONFLICT(shard) DO UPDATE SET watermark=excluded.watermark",
        (shard, int(watermark)),
    )


def load_checkpoints(con) -> dict[str, int]:
    ensure_cdc_tables(con)
    return {s: w for s, w in con.execute("SELECT shard,watermark FROM checkpoints")}


def mark_txn(con, txn_id: str, commit_ts: str, fingerprint: str) -> None:
    ensure_cdc_tables(con)
    con.execute("INSERT OR IGNORE INTO processed_txns(txn_id,commit_ts,fingerprint) "
                "VALUES(?,?,?)", (txn_id, commit_ts, fingerprint))


def mirror_count(con) -> int:
    ensure_cdc_tables(con)
    return con.execute("SELECT COUNT(*) FROM cdc_mirror").fetchone()[0]


def mirror_snapshot(con) -> dict[str, str]:
    ensure_cdc_tables(con)
    return {k: json.loads(d) for k, d in
            con.execute("SELECT key,data FROM cdc_mirror ORDER BY key")}
