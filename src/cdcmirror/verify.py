"""单一职责：对记录做单一职责变换：verify（happy-path 阶段标记，旧入口行为不变）。

另提供 CDC 镜像「源端 ↔ 目标端」一致性校验引擎：

- 同一比较边界：从读取批次（reader 高水位）、检查点（已确认消费位点）与目标存储
  （已应用位点）三方取得一致边界 boundary = min(reader_hw, checkpoint_hw)。
  边界之内必须账实相符；边界之外属于「尚未追平」(pending)，不是丢失。
- 分层摘要：表级 count / 主键范围 / 桶摘要先行比对（省成本）；桶摘要不一致时
  下钻行级摘要，定位到具体主键——摘要省成本但不丢定位差异的能力。
- 差异分类：pending（尚未消费）、lost（真正丢失）、mismatch（内容不符）、
  lost_delete（删除事件丢失）、unexpected_row（目标多出演化外的行）。
- 分片校验 + 部分失败隔离：单分片异常/超时只把本分片标记为 uncheckable，
  其余分片照常出结果。
- 断点续检：分片结果持久化，同一 run_id 重跑时复用已 ok 分片，仅补检未完成分片。
- 失败不污染：仅在校验通过时登记已确认批次；失败/中断的运行不改动既有确认记录；
  校验全程只读目标存储，绝不写入。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping


# ---------------------------------------------------------------- 旧入口（pipeline 阶段，行为不变）
def process(record):
    rec = dict(record)
    rec.setdefault("_stage_verify", "ok")
    return rec


# ---------------------------------------------------------------- 异常
class ShardTimeoutError(TimeoutError):
    """目标存储在分片读取时限内未响应。"""


# ---------------------------------------------------------------- 数据模型
@dataclass
class CdcEvent:
    shard: str
    seq: int
    tx: str
    table: str
    pk: str
    op: str                       # insert | update | delete
    payload: dict | None = None

    @staticmethod
    def from_dict(shard: str, d: Mapping[str, Any]) -> "CdcEvent":
        return CdcEvent(
            shard=shard,
            seq=int(d["seq"]),
            tx=str(d.get("tx", "")),
            table=str(d["table"]),
            pk=str(d["pk"]),
            op=str(d["op"]),
            payload=d.get("payload"),
        )


@dataclass
class ReadBatch:
    batch_id: str
    shard: str
    events: list[CdcEvent]

    @staticmethod
    def from_dict(d: Mapping[str, Any]) -> "ReadBatch":
        shard = str(d["shard"])
        return ReadBatch(
            batch_id=str(d["batch_id"]),
            shard=shard,
            events=[CdcEvent.from_dict(shard, e) for e in d.get("events", [])],
        )


@dataclass
class Discrepancy:
    kind: str                     # lost | mismatch | lost_delete | unexpected_row
    table: str
    pk: str
    seq: int | None = None        # 责任事件位点
    missed_events: list[int] = field(default_factory=list)
    expected: dict | None = None
    actual: dict | None = None
    detail: str = ""

    def to_dict(self) -> dict:
        return {
            "kind": self.kind, "table": self.table, "pk": self.pk,
            "seq": self.seq, "missed_events": list(self.missed_events),
            "expected": self.expected, "actual": self.actual, "detail": self.detail,
        }

    @staticmethod
    def from_dict(d: Mapping[str, Any]) -> "Discrepancy":
        return Discrepancy(
            kind=d["kind"], table=d["table"], pk=d["pk"], seq=d.get("seq"),
            missed_events=list(d.get("missed_events", [])),
            expected=d.get("expected"), actual=d.get("actual"),
            detail=d.get("detail", ""),
        )


@dataclass
class ShardReport:
    shard: str
    status: str                   # ok | mismatch | uncheckable
    boundary: int = 0
    tables: dict = field(default_factory=dict)
    tx_summaries: dict = field(default_factory=dict)
    discrepancies: list[Discrepancy] = field(default_factory=list)
    pending_events: list[int] = field(default_factory=list)
    repair: list[dict] = field(default_factory=list)
    target_high_water: int = 0
    warnings: list[str] = field(default_factory=list)
    error: str | None = None
    reused: bool = False          # 断点续检：本结果复用自上次运行

    def to_dict(self) -> dict:
        return {
            "shard": self.shard, "status": self.status, "boundary": self.boundary,
            "tables": self.tables, "tx_summaries": self.tx_summaries,
            "discrepancies": [d.to_dict() for d in self.discrepancies],
            "pending_events": list(self.pending_events),
            "repair": self.repair,
            "target_high_water": self.target_high_water,
            "warnings": list(self.warnings), "error": self.error,
            "reused": self.reused,
        }

    @staticmethod
    def from_dict(d: Mapping[str, Any]) -> "ShardReport":
        return ShardReport(
            shard=d["shard"], status=d["status"], boundary=d.get("boundary", 0),
            tables=dict(d.get("tables", {})),
            tx_summaries=dict(d.get("tx_summaries", {})),
            discrepancies=[Discrepancy.from_dict(x) for x in d.get("discrepancies", [])],
            pending_events=list(d.get("pending_events", [])),
            repair=list(d.get("repair", [])),
            target_high_water=d.get("target_high_water", 0),
            warnings=list(d.get("warnings", [])),
            error=d.get("error"), reused=bool(d.get("reused", False)),
        )


@dataclass
class VerifyReport:
    run_id: str
    status: str                   # ok | mismatch | incomplete
    boundary: dict[str, int]
    shards: dict[str, ShardReport]
    confirmed_batches: list[str]
    reused_shards: list[str]

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id, "status": self.status, "boundary": self.boundary,
            "shards": {s: r.to_dict() for s, r in sorted(self.shards.items())},
            "confirmed_batches": list(self.confirmed_batches),
            "reused_shards": list(self.reused_shards),
        }


# ---------------------------------------------------------------- 分层摘要：省成本且保定位
def _canon(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False)


def row_digest(payload: Any) -> str:
    """行级摘要：主键之外的内容指纹，供失配时逐行定位。"""
    return hashlib.sha256(_canon(payload).encode("utf-8")).hexdigest()


def bucket_digest(row_digests: Mapping[str, str]) -> str:
    """桶摘要：{pk: row_digest} 的有序聚合。比对只需一次哈希；
    不一致时仍持有每行摘要，可下钻定位到具体主键。"""
    h = hashlib.sha256()
    for pk in sorted(row_digests):
        h.update(pk.encode("utf-8"))
        h.update(b":")
        h.update(row_digests[pk].encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


# ---------------------------------------------------------------- 比较边界
def comparison_boundary(
    read_batches: Iterable[ReadBatch],
    checkpoints: Mapping[str, int],
) -> dict[str, int]:
    """从读取批次与检查点取得同一比较边界：min(reader 高水位, checkpoint 位点)。

    边界之内源端与目标端必须一致；边界之外是尚未消费的尾部（pending）。
    """
    reader_hw: dict[str, int] = {}
    for b in read_batches:
        for e in b.events:
            reader_hw[b.shard] = max(reader_hw.get(b.shard, 0), e.seq)
    shards = set(reader_hw) | set(checkpoints)
    return {s: min(reader_hw.get(s, 0), int(checkpoints.get(s, 0))) for s in shards}


# ---------------------------------------------------------------- 源端投影与差异定位
def _project(events: list[CdcEvent]):
    """应用边界内事件，得到边界处的期望镜像。

    返回 (expected, deletes, events_by_pk)：
    expected:     {(table, pk): {"payload":..., "seq":...}}
    deletes:      {(table, pk): 删除事件 seq}
    events_by_pk: {(table, pk): [边界内事件]}（用于缺失事件定位）
    """
    expected: dict[tuple[str, str], dict] = {}
    deletes: dict[tuple[str, str], int] = {}
    events_by_pk: dict[tuple[str, str], list[CdcEvent]] = {}
    for e in sorted(events, key=lambda x: x.seq):
        key = (e.table, e.pk)
        events_by_pk.setdefault(key, []).append(e)
        if e.op == "delete":
            expected.pop(key, None)
            deletes[key] = e.seq
        else:
            expected[key] = {"payload": e.payload, "seq": e.seq}
            deletes.pop(key, None)
    return expected, deletes, events_by_pk


def _diff(expected, target_map, deletes, events_by_pk) -> list[Discrepancy]:
    """行级下钻：桶摘要失配后在此定位到具体主键与责任事件。"""
    out: list[Discrepancy] = []
    for key in sorted(expected):
        table, pk = key
        exp = expected[key]
        seqs = [e.seq for e in events_by_pk[key]]
        got = target_map.get(key)
        if got is None:
            out.append(Discrepancy(
                kind="lost", table=table, pk=pk, seq=exp["seq"],
                missed_events=seqs, expected=exp["payload"], actual=None,
                detail="边界内事件在目标存储无对应行（真正丢失）",
            ))
        elif got["payload"] != exp["payload"]:
            missed = [s for s in seqs if s > int(got.get("source_seq") or 0)]
            out.append(Discrepancy(
                kind="mismatch", table=table, pk=pk, seq=exp["seq"],
                missed_events=missed, expected=exp["payload"], actual=got["payload"],
                detail="主键相同但内容不符",
            ))
    for key in sorted(set(target_map) - set(expected)):
        table, pk = key
        got = target_map[key]
        if key in deletes:
            out.append(Discrepancy(
                kind="lost_delete", table=table, pk=pk, seq=deletes[key],
                missed_events=[deletes[key]], expected=None, actual=got["payload"],
                detail="删除事件未在目标生效（行仍存在）",
            ))
        else:
            out.append(Discrepancy(
                kind="unexpected_row", table=table, pk=pk, seq=None,
                missed_events=[], expected=None, actual=got["payload"],
                detail="目标存在源端事件流无法解释的行",
            ))
    return out


def repair_suggestions(shard: str, discrepancies: list[Discrepancy],
                       pending_events: list[int], uncheckable: bool = False) -> list[dict]:
    """按差异类型给出修复建议；只建议，不执行。"""
    out: list[dict] = []
    if uncheckable:
        out.append({"action": "retry_shard", "shard": shard,
                    "reason": "分片不可检查（异常/超时），恢复后重检"})
        return out
    for d in discrepancies:
        if d.kind == "lost":
            out.append({"action": "replay_events", "table": d.table, "pk": d.pk,
                        "events": d.missed_events,
                        "reason": "重放边界内未生效事件"})
        elif d.kind == "mismatch":
            out.append({"action": "upsert_row", "table": d.table, "pk": d.pk,
                        "payload": d.expected,
                        "reason": "以源端边界处内容覆盖目标行"})
        elif d.kind == "lost_delete":
            out.append({"action": "delete_row", "table": d.table, "pk": d.pk,
                        "reason": "补应用丢失的删除事件"})
        elif d.kind == "unexpected_row":
            out.append({"action": "investigate_row", "table": d.table, "pk": d.pk,
                        "reason": "目标多出演化外的行，先排查越界写入再决定清理"})
    if pending_events:
        out.append({"action": "wait_recheck", "shard": shard,
                    "events": list(pending_events),
                    "reason": "尚未消费的尾部，等待追平后复检，不属于丢失"})
    return out


# ---------------------------------------------------------------- 目标存储抽象
class InMemoryTarget:
    """目标存储（内存版）：rows 形如
    {"shard","table","pk","payload","source_seq"}；unavailable_shards 模拟分片超时。"""

    def __init__(self, rows: Iterable[Mapping[str, Any]] = (),
                 unavailable_shards: Iterable[str] = ()):
        self._rows: dict[str, list[dict]] = {}
        for r in rows:
            row = {
                "table": str(r["table"]), "pk": str(r["pk"]),
                "payload": r.get("payload"),
                "source_seq": int(r.get("source_seq") or 0),
            }
            self._rows.setdefault(str(r.get("shard", "")), []).append(row)
        self._unavailable = set(unavailable_shards)

    def _guard(self, shard: str):
        if shard in self._unavailable:
            raise ShardTimeoutError(f"shard {shard}: 目标读取超时")

    def rows_for(self, shard: str) -> list[dict]:
        self._guard(shard)
        return [dict(r) for r in self._rows.get(shard, [])]

    def high_water(self, shard: str) -> int:
        self._guard(shard)
        return max((r["source_seq"] for r in self._rows.get(shard, [])), default=0)


class SqliteTarget:
    """目标存储（sqlite 版）：用于重启/回滚/接管演练的持久化目标。"""

    def __init__(self, con: sqlite3.Connection):
        self.con = con
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS target_rows("
            "shard TEXT NOT NULL, tbl TEXT NOT NULL, pk TEXT NOT NULL,"
            "payload TEXT, source_seq INTEGER NOT NULL,"
            "PRIMARY KEY(shard, tbl, pk))")
        self.con.commit()

    def replace_rows(self, shard: str, rows: Iterable[Mapping[str, Any]]):
        """整体替换分片内容（测试/回滚演练用；校验器自身永不调用）。"""
        self.con.execute("DELETE FROM target_rows WHERE shard=?", (shard,))
        for r in rows:
            self.con.execute(
                "INSERT INTO target_rows(shard,tbl,pk,payload,source_seq) VALUES(?,?,?,?,?)",
                (shard, str(r["table"]), str(r["pk"]),
                 _canon(r.get("payload")), int(r.get("source_seq") or 0)))
        self.con.commit()

    def rows_for(self, shard: str) -> list[dict]:
        cur = self.con.execute(
            "SELECT tbl,pk,payload,source_seq FROM target_rows WHERE shard=?", (shard,))
        return [{"table": t, "pk": p, "payload": json.loads(pl), "source_seq": s}
                for t, p, pl, s in cur.fetchall()]

    def high_water(self, shard: str) -> int:
        row = self.con.execute(
            "SELECT COALESCE(MAX(source_seq),0) FROM target_rows WHERE shard=?",
            (shard,)).fetchone()
        return int(row[0])


# ---------------------------------------------------------------- 校验器：分片校验 / 断点续检 / 失败隔离
class Verifier:
    """一致性校验器。

    状态（分片结果、已确认批次）持久化在 sqlite 连接中：
    - verify_shard_state(run_id, shard, ...)：断点续检的依据；
    - confirmed_batches(batch_id, ...)：只增不改，失败运行不得污染。
    """

    def __init__(self, con: sqlite3.Connection | None = None, *,
                 shard_timeout: float = 30.0):
        self._own = con is None
        self.con = con or sqlite3.connect(":memory:")
        self.shard_timeout = float(shard_timeout)
        self._ensure_schema()

    def _ensure_schema(self):
        self.con.executescript(
            "CREATE TABLE IF NOT EXISTS verify_shard_state("
            " run_id TEXT NOT NULL, shard TEXT NOT NULL, status TEXT NOT NULL,"
            " report_json TEXT NOT NULL, checked_at REAL NOT NULL,"
            " PRIMARY KEY(run_id, shard));"
            "CREATE TABLE IF NOT EXISTS confirmed_batches("
            " batch_id TEXT PRIMARY KEY, shard TEXT NOT NULL,"
            " run_id TEXT NOT NULL, confirmed_at REAL NOT NULL);")
        self.con.commit()

    def close(self):
        if self._own:
            self.con.close()

    # ---- 已确认批次（只增不改）----
    def confirmed_batches(self) -> list[str]:
        cur = self.con.execute("SELECT batch_id FROM confirmed_batches ORDER BY batch_id")
        return [r[0] for r in cur.fetchall()]

    def _confirm_batches(self, run_id: str, shard: str,
                         batches: list[ReadBatch], boundary: int):
        """仅登记「分片校验通过且事件全部落在边界内」的批次；
        跨边界（含未消费尾部）的批次不确认。INSERT OR IGNORE：绝不改动既有记录。"""
        for b in batches:
            if b.events and max(e.seq for e in b.events) <= boundary:
                self.con.execute(
                    "INSERT OR IGNORE INTO confirmed_batches"
                    "(batch_id,shard,run_id,confirmed_at) VALUES(?,?,?,?)",
                    (b.batch_id, shard, run_id, time.time()))

    # ---- 断点续检的状态读写 ----
    def _load_state(self, run_id: str, shard: str) -> ShardReport | None:
        row = self.con.execute(
            "SELECT report_json FROM verify_shard_state WHERE run_id=? AND shard=?",
            (run_id, shard)).fetchone()
        return ShardReport.from_dict(json.loads(row[0])) if row else None

    def _save_state(self, run_id: str, rep: ShardReport):
        self.con.execute(
            "INSERT INTO verify_shard_state(run_id,shard,status,report_json,checked_at)"
            " VALUES(?,?,?,?,?)"
            " ON CONFLICT(run_id,shard) DO UPDATE SET"
            " status=excluded.status, report_json=excluded.report_json,"
            " checked_at=excluded.checked_at",
            (run_id, rep.shard, rep.status,
             json.dumps(rep.to_dict(), ensure_ascii=False), time.time()))

    # ---- 单分片校验（只读目标存储）----
    def _check_shard(self, shard: str, batches: list[ReadBatch],
                     boundary: int, target) -> ShardReport:
        events = sorted((e for b in batches for e in b.events), key=lambda e: e.seq)
        in_boundary = [e for e in events if e.seq <= boundary]
        pending = [e.seq for e in events if e.seq > boundary]

        expected, deletes, events_by_pk = _project(in_boundary)
        trows = target.rows_for(shard)            # 可能抛 ShardTimeoutError
        target_map = {(r["table"], r["pk"]): r for r in trows}
        target_hw = target.high_water(shard)

        # 表级：行数 / 主键范围 / 桶摘要（先比摘要，失配再下钻行级）
        tables: dict[str, dict] = {}
        names = sorted({t for t, _ in expected} | {t for t, _ in target_map})
        for t in names:
            src = {pk: v["payload"] for (tb, pk), v in expected.items() if tb == t}
            tgt = {pk: r["payload"] for (tb, pk), r in target_map.items() if tb == t}
            src_dig = {pk: row_digest(p) for pk, p in src.items()}
            tgt_dig = {pk: row_digest(p) for pk, p in tgt.items()}
            tables[t] = {
                "count_source": len(src), "count_target": len(tgt),
                "pk_range_source": [min(src), max(src)] if src else None,
                "pk_range_target": [min(tgt), max(tgt)] if tgt else None,
                "digest_source": bucket_digest(src_dig),
                "digest_target": bucket_digest(tgt_dig),
                "digest_match": bucket_digest(src_dig) == bucket_digest(tgt_dig),
            }

        discrepancies = _diff(expected, target_map, deletes, events_by_pk)

        # 事务级汇总：每个事务的边界内事件数与未生效事件
        missed_by_pk = {d.pk: d.missed_events for d in discrepancies
                        if d.kind in ("lost", "mismatch", "lost_delete")}
        tx_summaries: dict[str, dict] = {}
        for e in in_boundary:
            ts = tx_summaries.setdefault(e.tx, {"events": 0, "missed": []})
            ts["events"] += 1
            if e.seq in missed_by_pk.get(e.pk, []):
                ts["missed"].append(e.seq)

        warnings: list[str] = []
        if target_hw > boundary:
            warnings.append(
                f"目标已应用位点 {target_hw} 越过比较边界 {boundary}（存在未确认消费）")
        seqs = [e.seq for e in in_boundary]
        if seqs:
            missing = sorted(set(range(min(seqs), max(seqs) + 1)) - set(seqs))
            if missing:
                warnings.append(f"读取批次在边界内存在序号空洞 {missing}（reader 侧疑似丢失）")

        status = "mismatch" if discrepancies else "ok"
        return ShardReport(
            shard=shard, status=status, boundary=boundary, tables=tables,
            tx_summaries=tx_summaries, discrepancies=discrepancies,
            pending_events=pending,
            repair=repair_suggestions(shard, discrepancies, pending),
            target_high_water=target_hw, warnings=warnings,
        )

    # ---- 一次校验运行 ----
    def run(self, *, run_id: str, read_batches, checkpoints,
            target, resume: bool = True) -> VerifyReport:
        batches = [b if isinstance(b, ReadBatch) else ReadBatch.from_dict(b)
                   for b in read_batches]
        boundary = comparison_boundary(batches, checkpoints)
        by_shard: dict[str, list[ReadBatch]] = {}
        for b in batches:
            by_shard.setdefault(b.shard, []).append(b)
        shards = sorted(set(by_shard) | set(boundary))

        reports: dict[str, ShardReport] = {}
        reused: list[str] = []
        for shard in shards:
            if resume:
                prior = self._load_state(run_id, shard)
                if prior is not None and prior.status == "ok":
                    prior.reused = True           # 断点续检：已 ok 分片直接复用
                    reports[shard] = prior
                    reused.append(shard)
                    continue
            shard_batches = by_shard.get(shard, [])
            started = time.monotonic()
            try:
                rep = self._check_shard(shard, shard_batches,
                                        boundary.get(shard, 0), target)
                if time.monotonic() - started > self.shard_timeout:
                    rep = ShardReport(
                        shard=shard, status="uncheckable",
                        boundary=boundary.get(shard, 0),
                        error=f"分片校验耗时超过时限 {self.shard_timeout}s",
                        repair=repair_suggestions(shard, [], [], uncheckable=True))
            except ShardTimeoutError as exc:      # 部分失败隔离：只标记本分片
                rep = ShardReport(
                    shard=shard, status="uncheckable",
                    boundary=boundary.get(shard, 0), error=str(exc),
                    repair=repair_suggestions(shard, [], [], uncheckable=True))
            except Exception as exc:              # noqa: BLE001 - 隔离一切分片内异常
                rep = ShardReport(
                    shard=shard, status="uncheckable",
                    boundary=boundary.get(shard, 0),
                    error=f"{type(exc).__name__}: {exc}",
                    repair=repair_suggestions(shard, [], [], uncheckable=True))
            self._save_state(run_id, rep)
            if rep.status == "ok":
                self._confirm_batches(run_id, shard, shard_batches,
                                      boundary.get(shard, 0))
            self.con.commit()                     # 每个分片落库一次：崩溃也可续检
            reports[shard] = rep

        if any(r.status == "mismatch" for r in reports.values()):
            status = "mismatch"
        elif any(r.status == "uncheckable" for r in reports.values()):
            status = "incomplete"
        else:
            status = "ok"
        return VerifyReport(
            run_id=run_id, status=status, boundary=boundary, shards=reports,
            confirmed_batches=self.confirmed_batches(), reused_shards=reused,
        )


# ---------------------------------------------------------------- 样例加载与 CLI
def load_case(path):
    """读取校验样例文件 → (case_meta, read_batches, checkpoints, target)。"""
    d = json.loads(Path(path).read_text(encoding="utf-8"))
    target = InMemoryTarget(d.get("target_rows", []),
                            unavailable_shards=d.get("unavailable_shards", []))
    return d, d.get("read_batches", []), d.get("checkpoints", {}), target


def run_case(path, *, run_id: str | None = None,
             con: sqlite3.Connection | None = None) -> VerifyReport:
    meta, batches, checkpoints, target = load_case(path)
    verifier = Verifier(con)
    try:
        return verifier.run(run_id=run_id or str(meta.get("case", "case")),
                            read_batches=batches, checkpoints=checkpoints,
                            target=target)
    finally:
        verifier.close()


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        print("usage: python -m cdcmirror.verify <case.json> [...]", file=sys.stderr)
        return 2
    exit_code = 0
    for path in args:
        report = run_case(path)
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
        if report.status != "ok":
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
