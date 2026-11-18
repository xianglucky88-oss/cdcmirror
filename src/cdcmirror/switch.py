"""单一职责：全量↔增量安全切换协议（switch）。

# 协议（先写清追平条件与回退后继续点，再谈机制）

## 阶段机
    全量切增量：FULL → PREPARE → CATCHUP → INCREMENTAL
    增量回全量：INCREMENTAL → PAUSE → RELOAD → RECATCHUP → INCREMENTAL
    回滚：PREPARE/CATCHUP → FULL；PAUSE → INCREMENTAL；RELOAD/RECATCHUP → PAUSE

## 切换点
`begin_switch(boundary)` 记录源边界 `source_boundary` 与切换点 `switch_point`：
位点 ≤ boundary 的数据由快照覆盖，一律跳过；位点 > boundary 的事件进入
暂停窗口（`switch_buffer` 缓冲，不直接应用）。

## 双阶段校验
1. 源侧（`begin_catchup`，见 `validate_source`）：未确认缓冲位点必须从
   boundary+1 连续，无空洞，否则拒绝进入追平。
2. 目标侧（`complete_switch` / `complete_reload`，见 `validate_target`）：
   必须满足追平条件。

## 追平条件
目标追平 ⇔ 缓冲区无未确认事件 且 已确认水位 ≥ catchup_goal。
已确认水位 = max(confirmed_position, source_boundary)；catchup_goal 为进入
追平时捕获的流头。追平期间新到事件一并入缓冲，须一并确认后才算追平。

## 回滚边界与回退后继续点
回滚边界 = 已确认水位（confirmed_position 与 source_boundary 的较大者）。
回滚丢弃未确认缓冲（已确认数据不受影响）；恢复后从 已确认水位 + 1 继续：
位点 ≤ 已确认水位 的事件一律跳过（不重复覆盖已确认数据），源端只需从
已确认水位 + 1 重发；再次 `begin_switch` 的 boundary 不得低于已确认水位。

## 不变量
1. 不漏应用：位点 > 已确认水位 的事件必被缓冲或应用，且按位点有序应用。
2. 不重复覆盖：位点 ≤ 已确认水位 的事件绝不写入目标。
3. 每次阶段转移写入 switch_journal（from/to/依据/已确认位点/时间）。

## 与现有 pipeline / 持久化的协作
- `process(record)` 是流水线阶段：复用 cdc_reader / checkpoint 的契约
  （不接管它们的业务实现），追加 `_stage_switch` / `_switch_phase` /
  `_switch_action`（apply/hold/skip）。
- 快照类记录（无位点，含旧输入迁移而来）只在 FULL/RELOAD 阶段放行
  （apply，由编排层落库）；其余阶段置 `_hold` 跳过——陈旧快照不得覆盖
  已确认数据。
- 事件类记录（带 pos）一律由本协议处置（缓冲或经 store 原语就地应用），
  并置 `_hold` 使编排层不再直写；删除事件经 `store.delete` 应用。
- 协议状态持久化在与业务库相同的 sqlite 中：`switch_state`（单行：
  阶段/源边界/切换点/目标确认位点）、`switch_journal`（转移依据）、
  `switch_buffer`（暂停窗口，按位点去重）。确认逐条提交：目标只确认
  半批时退出，恢复后从未确认位点继续，已确认部分绝不重放。
"""
import json
from datetime import datetime, timezone

from . import cdc_reader as _cdc_reader
from . import checkpoint as _checkpoint
from . import store
from .keyer import key_of
from .models import Record

SCHEMA_VERSION = 2

FULL = "FULL"
PREPARE = "PREPARE"
CATCHUP = "CATCHUP"
INCREMENTAL = "INCREMENTAL"
PAUSE = "PAUSE"
RELOAD = "RELOAD"
RECATCHUP = "RECATCHUP"

# 旧版（v1）阶段名映射：bulk=全量中，tail=增量中
_LEGACY_PHASES = {"bulk": FULL, "tail": INCREMENTAL}

_STATE_COLS = ("id", "schema_version", "phase", "source_boundary", "switch_point",
               "confirmed_position", "catchup_goal", "generation", "updated_at")


class SwitchError(Exception):
    """切换协议违规（阶段不符/源侧空洞/未追平/边界低于已确认水位等）。"""


def _now():
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------- 持久化

def _create_tables(con):
    con.execute(
        "CREATE TABLE IF NOT EXISTS switch_state("
        " id INTEGER PRIMARY KEY CHECK(id=1),"
        " schema_version INTEGER NOT NULL,"
        " phase TEXT NOT NULL,"
        " source_boundary INTEGER NOT NULL,"
        " switch_point INTEGER,"
        " confirmed_position INTEGER NOT NULL,"
        " catchup_goal INTEGER,"
        " generation INTEGER NOT NULL,"
        " updated_at TEXT NOT NULL)")
    con.execute(
        "CREATE TABLE IF NOT EXISTS switch_journal("
        " seq INTEGER PRIMARY KEY AUTOINCREMENT,"
        " generation INTEGER NOT NULL,"
        " from_phase TEXT NOT NULL,"
        " to_phase TEXT NOT NULL,"
        " reason TEXT NOT NULL,"
        " confirmed_position INTEGER NOT NULL,"
        " detail TEXT,"
        " at TEXT NOT NULL)")
    con.execute(
        "CREATE TABLE IF NOT EXISTS switch_buffer("
        " position INTEGER PRIMARY KEY,"
        " op TEXT NOT NULL,"
        " record TEXT NOT NULL,"
        " applied INTEGER NOT NULL DEFAULT 0,"
        " arrived_at TEXT NOT NULL)")


def _migrate_legacy(con):
    """旧版 switch_state(id, phase, boundary) → v2。

    v1 位点语义：boundary 即源端已镜像位点 → 迁移后 confirmed_position := boundary。
    旧表保留为 switch_state_v1 备查；迁移本身记入 journal。
    """
    rows = con.execute("SELECT id, phase, boundary FROM switch_state").fetchall()
    con.execute("ALTER TABLE switch_state RENAME TO switch_state_v1")
    _create_tables(con)
    for _id, phase, boundary in rows:
        new_phase = _LEGACY_PHASES.get(str(phase).lower(), FULL)
        boundary = int(boundary or 0)
        con.execute(
            "INSERT INTO switch_state(id, schema_version, phase, source_boundary,"
            " switch_point, confirmed_position, catchup_goal, generation, updated_at)"
            " VALUES(1,?,?,?,?,?,NULL,1,?)",
            (SCHEMA_VERSION, new_phase, boundary, boundary, boundary, _now()))
        con.execute(
            "INSERT INTO switch_journal(generation, from_phase, to_phase, reason,"
            " confirmed_position, detail, at) VALUES(1,?,?,?,?,?,?)",
            (new_phase, new_phase,
             f"migrate: legacy switch_state v1 phase={phase!r} → {new_phase}",
             boundary, "schema v1→v2；confirmed_position := boundary", _now()))


def _ensure_schema(con):
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='switch_state'"
    ).fetchone()
    if row:
        cols = {r[1] for r in con.execute("PRAGMA table_info(switch_state)")}
        if "schema_version" not in cols:
            _migrate_legacy(con)
    else:
        _create_tables(con)
    if con.execute("SELECT COUNT(*) FROM switch_state").fetchone()[0] == 0:
        con.execute(
            "INSERT INTO switch_state(id, schema_version, phase, source_boundary,"
            " switch_point, confirmed_position, catchup_goal, generation, updated_at)"
            " VALUES(1,?,?,0,NULL,0,NULL,0,?)",
            (SCHEMA_VERSION, FULL, _now()))


def _load_state(con):
    row = con.execute(
        "SELECT id, schema_version, phase, source_boundary, switch_point,"
        " confirmed_position, catchup_goal, generation, updated_at"
        " FROM switch_state WHERE id=1").fetchone()
    return dict(zip(_STATE_COLS, row))


def _watermark(st):
    """已确认水位：位点 ≤ 水位 的数据目标已一致，绝不重写。"""
    return max(st["confirmed_position"], st["source_boundary"])


def _pending(con):
    return con.execute(
        "SELECT COUNT(*) FROM switch_buffer WHERE applied=0").fetchone()[0]


def _transition(con, st, to_phase, reason, detail=None, **sets):
    """阶段转移：先记 journal（依据），再更新状态行，同事务提交。"""
    con.execute(
        "INSERT INTO switch_journal(generation, from_phase, to_phase, reason,"
        " confirmed_position, detail, at) VALUES(?,?,?,?,?,?,?)",
        (st["generation"], st["phase"], to_phase, reason,
         st["confirmed_position"], detail, _now()))
    cols = {"phase": to_phase, "updated_at": _now(), **sets}
    con.execute(
        "UPDATE switch_state SET " + ", ".join(f"{k}=?" for k in cols) + " WHERE id=1",
        tuple(cols.values()))
    con.commit()


# ---------------------------------------------------------------- 状态查询

def status(con):
    """当前协议状态（含待确认事件数与已确认水位）。"""
    _ensure_schema(con)
    con.commit()
    st = _load_state(con)
    st["pending"] = _pending(con)
    st["watermark"] = _watermark(st)
    return st


def resume(con):
    """恢复后继续：返回状态并给出下一步动作提示。"""
    st = status(con)
    phase = st["phase"]
    if phase in (CATCHUP, RECATCHUP):
        nxt = "confirm" if st["pending"] else "complete"
    elif phase == PREPARE:
        nxt = "begin_catchup"
    elif phase == PAUSE:
        nxt = "begin_snapshot"
    elif phase == RELOAD:
        nxt = "begin_recatchup"
    elif phase == FULL:
        nxt = "begin_switch"
    else:
        nxt = "steady"
    st["next"] = nxt
    return st


def transitions(con):
    """阶段转移日志（每次转移的依据）。"""
    _ensure_schema(con)
    con.commit()
    rows = con.execute(
        "SELECT seq, generation, from_phase, to_phase, reason,"
        " confirmed_position, detail, at FROM switch_journal ORDER BY seq").fetchall()
    cols = ("seq", "generation", "from_phase", "to_phase", "reason",
            "confirmed_position", "detail", "at")
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------- 全量 → 增量

def begin_switch(con, boundary, reason="全量完成，开始切换"):
    """记录切换点：FULL → PREPARE。重复发起（同边界）幂等。"""
    _ensure_schema(con)
    st = _load_state(con)
    boundary = int(boundary)
    if st["phase"] in (PREPARE, CATCHUP):
        if st["source_boundary"] == boundary:
            return {**status(con), "dedup": True}
        raise SwitchError(
            f"切换已在进行（boundary={st['source_boundary']}）；变更边界须先 rollback")
    if st["phase"] != FULL:
        raise SwitchError(f"当前阶段 {st['phase']} 不能发起全量→增量切换")
    if boundary < _watermark(st):
        raise SwitchError(
            f"boundary={boundary} 低于已确认水位 {_watermark(st)}；"
            "恢复后请从已确认水位继续")
    # 快照覆盖位点 ≤ boundary：缓冲区内对应事件标记已确认（跳过，不重放）
    con.execute("UPDATE switch_buffer SET applied=1 WHERE position<=?", (boundary,))
    head = con.execute(
        "SELECT MAX(position) FROM switch_buffer WHERE applied=0").fetchone()[0]
    switch_point = head if head is not None else boundary
    _transition(con, st, PREPARE, reason,
                detail=f"切换点 boundary={boundary}, switch_point={switch_point}",
                source_boundary=boundary, switch_point=switch_point,
                catchup_goal=None, generation=st["generation"] + 1)
    return status(con)


def validate_source(con):
    """双阶段校验·源侧：未确认缓冲位点必须从 boundary+1 连续（无空洞）。"""
    _ensure_schema(con)
    st = _load_state(con)
    positions = [r[0] for r in con.execute(
        "SELECT position FROM switch_buffer WHERE applied=0 ORDER BY position")]
    base = st["source_boundary"]
    expected = list(range(base + 1, base + 1 + len(positions)))
    ok = positions == expected
    return {"ok": ok, "boundary": base, "buffered": len(positions),
            "head": positions[-1] if positions else base,
            "gap": None if ok else {"expected": expected[:3], "actual": positions[:3]}}


def begin_catchup(con, reason="源侧校验通过，进入追平"):
    """源侧校验通过后 PREPARE → CATCHUP，并捕获追平目标 catchup_goal。"""
    _ensure_schema(con)
    st = _load_state(con)
    if st["phase"] == CATCHUP:
        return {**status(con), "dedup": True}
    if st["phase"] != PREPARE:
        raise SwitchError(f"当前阶段 {st['phase']} 不能进入追平")
    v = validate_source(con)
    if not v["ok"]:
        raise SwitchError(f"源侧校验失败：位点不连续 {v['gap']}；请补齐或 rollback")
    _transition(con, st, CATCHUP, reason,
                detail=f"位点自 {v['boundary'] + 1} 连续，流头={v['head']}",
                catchup_goal=v["head"])
    return status(con)


def confirm_batch(con, limit=None):
    """目标追平确认：按位点顺序应用缓冲事件，逐条提交。

    逐条提交保证：目标只确认半批时退出，恢复后从未确认位点继续，
    已确认事件绝不重放（不重复覆盖已确认数据）。
    """
    _ensure_schema(con)
    st = _load_state(con)
    if st["phase"] not in (CATCHUP, RECATCHUP):
        raise SwitchError(f"当前阶段 {st['phase']} 无追平确认")
    rows = con.execute(
        "SELECT position, op, record FROM switch_buffer"
        " WHERE applied=0 ORDER BY position").fetchall()
    if limit is not None:
        rows = rows[:int(limit)]
    applied, skipped = [], []
    confirmed = st["confirmed_position"]
    for pos, op, payload in rows:
        if pos <= confirmed:
            # 位点 ≤ 已确认位点：只标记，不写入目标
            con.execute("UPDATE switch_buffer SET applied=1 WHERE position=?", (pos,))
            skipped.append(pos)
        else:
            rec = json.loads(payload)
            key = key_of(rec)
            if op == "delete":
                store.delete(con, key)
            else:
                store.upsert(con, Record(key=key, data=rec))
            con.execute("UPDATE switch_buffer SET applied=1 WHERE position=?", (pos,))
            confirmed = pos
            applied.append(pos)
        con.execute(
            "UPDATE switch_state SET confirmed_position=?, updated_at=? WHERE id=1",
            (confirmed, _now()))
        con.commit()  # 逐条提交：半批退出可恢复
    return {"applied": applied, "skipped": skipped, "confirmed_position": confirmed}


def validate_target(con, expected_rows=None):
    """双阶段校验·目标侧：追平条件 = 缓冲清空 且 已确认水位 ≥ 追平目标。"""
    _ensure_schema(con)
    st = _load_state(con)
    pending = _pending(con)
    goal = st["catchup_goal"]
    wm = _watermark(st)
    checks = {
        "buffer_drained": pending == 0,
        "goal_reached": goal is None or wm >= goal,
        "rows_match": expected_rows is None or store.count(con) == expected_rows,
    }
    return {"ok": all(checks.values()), "checks": checks, "pending": pending,
            "watermark": wm, "catchup_goal": goal}


def _complete(con, from_phase, expected_rows, reason):
    _ensure_schema(con)
    st = _load_state(con)
    if st["phase"] != from_phase:
        raise SwitchError(f"当前阶段 {st['phase']} 不能完成切换（期望 {from_phase}）")
    v = validate_target(con, expected_rows=expected_rows)
    if not v["ok"]:
        raise SwitchError(f"目标未追平：{v['checks']}；请 confirm_batch 或 rollback")
    _transition(con, st, INCREMENTAL, reason,
                detail=f"目标追平：watermark={v['watermark']}, goal={v['catchup_goal']}",
                catchup_goal=None)
    return status(con)


def complete_switch(con, expected_rows=None, reason="目标追平，切入增量"):
    """目标侧校验通过后 CATCHUP → INCREMENTAL。"""
    return _complete(con, CATCHUP, expected_rows, reason)


# ---------------------------------------------------------------- 增量 → 全量

def begin_reload(con, reason="回全量：暂停增量应用"):
    """打开暂停窗口：INCREMENTAL → PAUSE。重复发起幂等。"""
    _ensure_schema(con)
    st = _load_state(con)
    if st["phase"] in (PAUSE, RELOAD, RECATCHUP):
        return {**status(con), "dedup": True}
    if st["phase"] != INCREMENTAL:
        raise SwitchError(f"当前阶段 {st['phase']} 不能发起增量→全量")
    _transition(con, st, PAUSE, reason,
                detail=f"暂停窗口开启，pause_point={st['confirmed_position']}",
                switch_point=st["confirmed_position"],
                generation=st["generation"] + 1)
    return status(con)


def begin_snapshot(con, reason="全量重载开始"):
    """PAUSE → RELOAD：全量重载进行，期间事件持续缓冲。"""
    _ensure_schema(con)
    st = _load_state(con)
    if st["phase"] == RELOAD:
        return {**status(con), "dedup": True}
    if st["phase"] != PAUSE:
        raise SwitchError(f"当前阶段 {st['phase']} 不能开始全量重载")
    _transition(con, st, RELOAD, reason,
                detail="暂停窗口内事件持续缓冲，重载期间不改写增量位点")
    return status(con)


def begin_recatchup(con, new_boundary, reason="重载完成，进入再追平"):
    """RELOAD → RECATCHUP：记录新快照边界，≤ 边界的缓冲事件被快照覆盖（跳过）。"""
    _ensure_schema(con)
    st = _load_state(con)
    new_boundary = int(new_boundary)
    if st["phase"] == RECATCHUP:
        if st["source_boundary"] == new_boundary:
            return {**status(con), "dedup": True}
        raise SwitchError("再追平边界冲突；须先 rollback")
    if st["phase"] != RELOAD:
        raise SwitchError(f"当前阶段 {st['phase']} 不能进入再追平")
    if new_boundary < _watermark(st):
        raise SwitchError(
            f"new_boundary={new_boundary} 低于已确认水位 {_watermark(st)}")
    # 新快照覆盖 ≤ new_boundary：对应缓冲事件标记已确认（跳过，不重放）
    con.execute("UPDATE switch_buffer SET applied=1 WHERE position<=?", (new_boundary,))
    positions = [r[0] for r in con.execute(
        "SELECT position FROM switch_buffer WHERE applied=0 ORDER BY position")]
    expected = list(range(new_boundary + 1, new_boundary + 1 + len(positions)))
    if positions != expected:
        con.commit()  # 落盘快照覆盖标记，但不转移阶段
        raise SwitchError(
            f"源侧校验失败：位点不连续（期望自 {new_boundary + 1} 连续，"
            f"实际 {positions[:3]}）；请补齐或 rollback")
    goal = positions[-1] if positions else new_boundary
    _transition(con, st, RECATCHUP, reason,
                detail=f"新快照边界 {new_boundary}，追平目标 {goal}",
                source_boundary=new_boundary, catchup_goal=goal)
    return status(con)


def complete_reload(con, expected_rows=None, reason="再追平完成，回到增量"):
    """目标侧校验通过后 RECATCHUP → INCREMENTAL。"""
    return _complete(con, RECATCHUP, expected_rows, reason)


# ---------------------------------------------------------------- 回滚

def rollback(con, reason="手动回滚"):
    """回滚到回滚边界（已确认水位）：丢弃未确认缓冲，回到来源稳定阶段。

    恢复后从 已确认水位 + 1 继续；位点 ≤ 水位 的事件一律跳过。
    """
    _ensure_schema(con)
    st = _load_state(con)
    back = {PREPARE: FULL, CATCHUP: FULL, PAUSE: INCREMENTAL,
            RELOAD: PAUSE, RECATCHUP: PAUSE}.get(st["phase"])
    if back is None:
        raise SwitchError(f"当前阶段 {st['phase']} 无可回滚")
    dropped = _pending(con)
    con.execute("DELETE FROM switch_buffer WHERE applied=0")
    wm = _watermark(st)
    _transition(con, st, back, reason,
                detail=f"回滚边界={wm}（已确认水位）；丢弃未确认事件 {dropped} 条；"
                       f"恢复后从 {wm + 1} 继续",
                catchup_goal=None)
    return status(con)


# ---------------------------------------------------------------- 流水线阶段

def _normalize_record(rec):
    """旧输入迁移：无 op/pos 信封的历史记录升级为当前信封。

    - 缺 op：显式 `_deleted` → delete；带 pos → update；否则 → snapshot。
    - 有 op（非 snapshot）但缺 pos：无法安全定序，拒绝。
    """
    if "op" not in rec:
        if rec.get("_deleted"):
            rec["op"] = "delete"
        elif rec.get("pos") is not None:
            rec["op"] = "update"
        else:
            rec["op"] = "snapshot"
        rec["_migrated"] = True
    if rec["op"] != "snapshot" and rec.get("pos") is None:
        raise SwitchError(f"事件记录缺少位点 pos（op={rec['op']!r}），无法安全定序")
    if rec.get("pos") is not None:
        rec["pos"] = int(rec["pos"])
    return rec


def _apply_event(con, rec):
    """就地应用事件并推进目标确认位点（与状态更新同事务）。"""
    key = key_of(rec)
    if rec["op"] == "delete":
        store.delete(con, key)
    else:
        store.upsert(con, Record(key=key, data=rec))
    con.execute(
        "UPDATE switch_state SET confirmed_position=?, updated_at=? WHERE id=1",
        (rec["pos"], _now()))


def _buffer_event(con, rec):
    """暂停窗口：事件入缓冲（按位点去重，等待追平确认）。"""
    con.execute(
        "INSERT OR IGNORE INTO switch_buffer(position, op, record, applied, arrived_at)"
        " VALUES(?,?,?,0,?)",
        (rec["pos"], rec["op"],
         json.dumps(rec, ensure_ascii=False, sort_keys=True), _now()))


def process(record, con=None):
    """流水线阶段入口：按当前切换阶段决定记录处置方式。

    复用 cdc_reader / checkpoint 的契约（不接管其业务实现）。
    返回记录附带 `_stage_switch` / `_switch_phase` / `_switch_action`；
    事件类记录与暂缓快照一律置 `_hold`，编排层不再直写。
    """
    own = con is None
    con = con or store.connect()
    try:
        _ensure_schema(con)
        rec = _normalize_record(dict(record))
        rec = _cdc_reader.process(rec)   # 复用读取契约
        rec = _checkpoint.process(rec)   # 复用检查点契约
        st = _load_state(con)
        phase = st["phase"]
        pos = rec.get("pos")
        if pos is None:
            # 快照类（含旧输入迁移而来）：只在大批量阶段由编排层落库
            if phase in (FULL, RELOAD):
                action = "apply"
            else:
                action = "skip"
                rec["_hold"] = True  # 陈旧快照不得覆盖已确认数据
        else:
            if pos <= _watermark(st):
                action = "skip"  # 已确认/已被快照覆盖：不重复覆盖
            elif phase == INCREMENTAL and _pending(con) == 0:
                _apply_event(con, rec)
                action = "apply"
            else:
                _buffer_event(con, rec)
                action = "hold"
            rec["_hold"] = True  # 事件类一律由本协议处置，不经编排层直写
        rec["_stage_switch"] = "ok"
        rec["_switch_phase"] = phase
        rec["_switch_action"] = action
        con.commit()
        return rec
    finally:
        if own:
            con.close()
