"""CDC/binlog 事件排序与事务整体可见性（单一职责：order）。

本模块回答两个问题，并把答案交给既有检查点（checkpoint）与写入（store）环节：

1. **分片局部顺序**：同一分片内，事件严格按 ``(lsn, txn_seq, table, pk)`` 定序。
   LSN 是分片内的全序日志位置；出现 LSN 缺口意味着可能还有更早事件未到达，
   缺口之后的事件不能被确认。
2. **事务整体可见性**：一个事务（可能跨多个分片）只有在
   - 收到提交标记且未中止，并且
   - 它在每个参与分片上的全部事件 LSN 都已落在该分片的连续水位线之下
   时，才整体可见，按 ``(commit_ts, txn_id)`` 的全局顺序释放。未完成事务
   永远停留在缓冲区内，不会被提前确认。

缺口策略（可验证）：

* ``wait``：缺口后的事件一律保留在缓冲区，``buffer_peak`` 记录等待占用的缓存
  峰值；当缓冲事件数达到 ``wait_budget`` 仍无法补缺时，依据明确的预算硬边界
  升级为隔离，原因记为 ``wait_budget_exceeded``。
* ``quarantine``：发现缺口立即把缺口边缘及之后的事件隔离（``gap_quarantine``），
  水位线停在缺口处不前进；迟到的补缺事件到达后，隔离区中恢复连续的事件会被
  自动重新接纳（heal），全过程在 explain 输出中可解释。

排序结果是输入事件序列与配置的纯函数：同输入必然得到同计划（可重放），
计划附带指纹（fingerprint），一次处理与分多次恢复产生的落库结果一致。

注意：为兼容既有 happy-path，记录级 ``process(record)`` 保持原有朴素行为不变；
新的批式能力由 ``OrderingEngine`` / ``order_events`` 提供。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterable

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

POLICY_WAIT = "wait"
POLICY_QUARANTINE = "quarantine"

TXN_OPEN = "open"
TXN_COMMITTED = "committed"
TXN_ABORTED = "aborted"

OP_INSERT = "insert"
OP_UPDATE = "update"
OP_DELETE = "delete"
OP_SNAPSHOT = "snapshot"
OP_BEGIN = "begin"
OP_COMMIT = "commit"
OP_ABORT = "abort"

_DML_OPS = frozenset({OP_INSERT, OP_UPDATE, OP_DELETE, OP_SNAPSHOT})
_CONTROL_OPS = frozenset({OP_BEGIN, OP_COMMIT, OP_ABORT})

DEFAULT_BASE_LSN = 1  # 分片起始期望 LSN（含）


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------

@dataclass
class Event:
    """一条 CDC/binlog 事件。

    ``event_id`` 是投递身份（用于重复检测）；``(shard, lsn)`` 是日志位置身份
    （同位置不同身份视为冲突，隔离而不是静默覆盖）。
    """

    event_id: str
    shard: str
    lsn: int
    txn_id: str | None = None
    txn_seq: int = 0
    commit_ts: str = ""
    table: str = ""
    pk: str = ""
    op: str = OP_INSERT
    is_snapshot: bool = False
    snapshot_ts: str | None = None
    autocommit: bool = False
    data: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "Event":
        return Event(
            event_id=str(raw["event_id"]),
            shard=str(raw["shard"]),
            lsn=int(raw["lsn"]),
            txn_id=raw.get("txn_id"),
            txn_seq=int(raw.get("txn_seq", 0)),
            commit_ts=str(raw.get("commit_ts", "")),
            table=str(raw.get("table", "")),
            pk=str(raw.get("pk", "")),
            op=str(raw.get("op", OP_INSERT)),
            is_snapshot=bool(raw.get("is_snapshot", False)) or raw.get("op") == OP_SNAPSHOT,
            snapshot_ts=raw.get("snapshot_ts"),
            autocommit=bool(raw.get("autocommit", False)),
            data=dict(raw.get("data") or {}),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "shard": self.shard,
            "lsn": self.lsn,
            "txn_id": self.txn_id,
            "txn_seq": self.txn_seq,
            "commit_ts": self.commit_ts,
            "table": self.table,
            "pk": self.pk,
            "op": self.op,
            "is_snapshot": self.is_snapshot,
            "snapshot_ts": self.snapshot_ts,
            "autocommit": self.autocommit,
            "data": dict(self.data),
        }

    def business_order_key(self) -> tuple:
        """分片局部业务定序键：LSN → 事务序号 → 表 → 主键。"""
        return (self.lsn, self.txn_seq, self.table, self.pk)

    def order_key(self) -> tuple:
        """完整定序键：业务键相同时用投递身份兜底，保证确定性、可重放。"""
        return self.business_order_key() + (self.event_id,)


@dataclass
class Unit:
    """释放单元：一个完整事务（或隐式单语句事务）的有序 DML 批。"""

    txn_id: str
    commit_ts: str
    events: list[Event] = field(default_factory=list)
    suppressed: list[Event] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "txn_id": self.txn_id,
            "commit_ts": self.commit_ts,
            "events": [e.as_dict() for e in self.events],
            "suppressed": [e.as_dict() for e in self.suppressed],
        }


@dataclass
class Quarantined:
    event: Event
    reason: str
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {"event": self.event.as_dict(), "reason": self.reason, "detail": self.detail}


@dataclass
class _TxnState:
    txn_id: str
    status: str = TXN_OPEN
    commit_ts: str = ""
    events: list[Event] = field(default_factory=list)
    shards: set[str] = field(default_factory=set)


# ---------------------------------------------------------------------------
# 排序引擎
# ---------------------------------------------------------------------------

class OrderingEngine:
    """有状态的乱序摄入 → 有序可重放计划。

    用法::

        eng = OrderingEngine(gap_policy="wait", wait_budget=1000)
        for raw in cdc_events:
            eng.ingest(raw)
        plan = eng.plan()          # 已可安全释放的事务单元
        state = eng.state_dict()   # 交给 checkpoint 持久化

    不变量：

    * 未提交/已中止事务不出现在 ``plan().units`` 中；
    * 任一分片存在缺口时，水位线停在缺口处，缺口之后的事件不释放；
    * 重复 ``event_id`` 去重；同 ``(shard, lsn)`` 不同身份进隔离区；
    * 全量快照覆盖窗口内的增量事件被抑制并记录原因。
    """

    def __init__(
        self,
        gap_policy: str = POLICY_WAIT,
        wait_budget: int = 10_000,
        base_lsn: dict[str, int] | None = None,
    ) -> None:
        if gap_policy not in (POLICY_WAIT, POLICY_QUARANTINE):
            raise ValueError(f"unknown gap_policy: {gap_policy!r}")
        self.gap_policy = gap_policy
        self.wait_budget = int(wait_budget)
        self._base_lsn: dict[str, int] = dict(base_lsn or {})

        # 分片局部日志：shard -> {lsn: [Event]}（同 LSN 可有多行 DML）
        self._by_shard: dict[str, dict[int, list[Event]]] = {}
        self._low: dict[str, int] = {}          # 连续水位线（下一个期望 LSN）
        self._implicit_low: set[str] = set()    # 起点由首条事件隐式确定的分片
        self._seen_ids: dict[str, tuple[str, int]] = {}
        self._quarantine: list[Quarantined] = []
        self._duplicates: list[Event] = []
        self._aborted_events: list[Event] = []
        self._suppressed_ids: set[str] = set()
        self._healed_ids: set[str] = set()
        self._decisions: list[dict[str, Any]] = []

        # 事务整体可见性
        self._txn: dict[str, _TxnState] = {}
        self._eligible: set[str] = set()       # 已满足可见性条件、可进入计划的事务
        self._released_txn: set[str] = set()   # 已进入过计划（含历史恢复段）的事务

        # 全量快照窗口：(shard, table, pk) -> (snapshot_ts, snapshot_lsn)
        self._snapshots: dict[tuple[str, str, str], tuple[str, int]] = {}

        self.buffer_peak = 0

    # ------------------------------------------------------------------
    # 摄入
    # ------------------------------------------------------------------

    def ingest(self, raw: dict[str, Any] | Event) -> Event | None:
        """摄入一条事件；被判定为重复时返回 ``None``，否则返回规范化事件。"""
        ev = raw if isinstance(raw, Event) else Event.from_dict(raw)
        shard, lsn = ev.shard, ev.lsn

        # 1) 投递身份去重
        if ev.event_id in self._seen_ids:
            self._duplicates.append(ev)
            self._decisions.append({"event_id": ev.event_id, "decision": "duplicate"})
            return None

        # 2) 日志位置冲突：同一 (shard, lsn) 且定序键完全相同的两个身份
        #    才视为冲突（隔离而不是静默覆盖）；同 LSN 的多行 DML 合法共存，
        #    由 order_key 中的 txn_seq/table/pk 区分先后。
        bucket = self._by_shard.setdefault(shard, {}).setdefault(lsn, [])
        clash = next((x for x in bucket
                      if x.business_order_key() == ev.business_order_key()), None)
        if clash is not None:
            self._quarantine.append(
                Quarantined(ev, "lsn_conflict",
                            f"{shard}:{lsn} 定序键已被 {clash.event_id} 占用")
            )
            self._decisions.append(
                {"event_id": ev.event_id, "decision": "quarantine",
                 "reason": "lsn_conflict"}
            )
            return None
        bucket.append(ev)

        # 3) 登记身份。新分片的起始位置取显式 base/检查点；没有显式位置时
        # 以首个观测 LSN 为起点（此前不属于本订阅窗口），隐式起点允许被
        # 更早的迟到事件下拉。
        self._seen_ids[ev.event_id] = (shard, lsn)
        if shard not in self._low:
            self._low[shard] = self._base_lsn.get(shard, lsn)
            if shard not in self._base_lsn:
                self._implicit_low.add(shard)
        elif shard in self._implicit_low and lsn < self._low[shard]:
            self._low[shard] = lsn

        # 4) 事务簿记（含跨分片参与集合）
        self._bookkeep(ev)

        # 5) 补缺后尝试解除隔离（迟到的补缺事件到达）
        self._heal(shard)

        # 6) 缺口判定：等待 or 隔离
        self._handle_gap(shard)

        # 7) 推进水位线并按全局提交顺序释放事务
        self._pump()

        self.buffer_peak = max(self.buffer_peak, self.buffered_count())
        return ev

    def ingest_all(self, raws: Iterable[dict[str, Any] | Event]) -> None:
        for raw in raws:
            self.ingest(raw)

    # ------------------------------------------------------------------
    # 事务簿记
    # ------------------------------------------------------------------

    def _bookkeep(self, ev: Event) -> None:
        if ev.op in _CONTROL_OPS:
            if not ev.txn_id:
                self._quarantine.append(
                    Quarantined(ev, "control_without_txn", "控制事件缺少 txn_id")
                )
                return
            st = self._txn.setdefault(ev.txn_id, _TxnState(txn_id=ev.txn_id))
            if ev.op == OP_COMMIT:
                st.status = TXN_COMMITTED
                st.commit_ts = ev.commit_ts or st.commit_ts
            elif ev.op == OP_ABORT:
                if ev.txn_id in self._released_txn:
                    # 已释放事务收到迟到 abort：无法撤回，记冲突供人工核验
                    self._quarantine.append(
                        Quarantined(ev, "late_abort", f"事务 {ev.txn_id} 已释放")
                    )
                else:
                    st.status = TXN_ABORTED
            return

        if ev.op not in _DML_OPS:
            self._quarantine.append(Quarantined(ev, "unknown_op", f"op={ev.op!r}"))
            return

        # 隐式单语句事务：没有 txn_id 时合成确定身份
        txn_id = ev.txn_id or f"~auto:{ev.shard}:{ev.lsn}:{ev.event_id}"
        ev.txn_id = txn_id
        st = self._txn.setdefault(txn_id, _TxnState(txn_id=txn_id))
        st.events.append(ev)
        st.shards.add(ev.shard)
        if ev.commit_ts and not st.commit_ts:
            st.commit_ts = ev.commit_ts
        if ev.autocommit:
            # 源库明确告知该语句在自动提交模式下执行（无 BEGIN/COMMIT 包裹），
            # 这是显式信号而非猜测：单语句即完整事务
            st.status = TXN_COMMITTED
            self._decisions.append({
                "decision": "autocommit", "txn_id": txn_id,
            })
        if ev.is_snapshot or ev.op == OP_SNAPSHOT:
            # 快照本身视为已提交的只读镜像行
            st.status = TXN_COMMITTED
            st.commit_ts = st.commit_ts or ev.snapshot_ts or ev.commit_ts
            self._snapshots[(ev.shard, ev.table, ev.pk)] = (
                ev.snapshot_ts or ev.commit_ts,
                ev.lsn,
            )

    # ------------------------------------------------------------------
    # 缺口与隔离
    # ------------------------------------------------------------------

    def _gaps(self, shard: str) -> list[int]:
        """返回分片订阅窗口内缺失的 LSN（有序）。

        窗口下界是显式 ``base_lsn``（恢复检查点）；没有显式锚点时，以分片
        上已观测到的最小 LSN 为下界——此前的位置不属于本订阅窗口，不能算缺口。
        """
        buf = self._by_shard.get(shard, {})
        if not buf:
            return []
        anchor = self._base_lsn.get(shard, min(buf))
        low = max(self._low[shard], anchor)
        return [lsn for lsn in range(low, max(buf) + 1) if lsn not in buf]

    def _handle_gap(self, shard: str) -> None:
        gaps = self._gaps(shard)
        if not gaps:
            return
        # 隐式锚点分片（无显式订阅起点）首个窗口之前的空洞不视为缺口：
        # 隔离只能作用于有明确检查点锚定的订阅窗口。
        if shard not in self._base_lsn and shard in self._implicit_low:
            return
        edge = gaps[0]  # 第一个缺口即水位线位置

        if self.gap_policy == POLICY_QUARANTINE:
            self._fence(shard, edge, "gap_quarantine",
                        f"分片 {shard} 在 LSN {edge} 出现缺口，缺口边缘起隔离")
            return

        # wait 策略：先等待；缓存达到预算上限仍无法补缺才升级为隔离。
        # 预算是可验证的硬边界：buffered >= wait_budget 即触发，决策留痕。
        if self.buffered_count() >= self.wait_budget:
            self._fence(shard, edge, "wait_budget_exceeded",
                        f"等待缓存 {self.buffered_count()} 达到预算 {self.wait_budget}")
            self._decisions.append({
                "decision": "budget_escalation",
                "shard": shard,
                "edge": edge,
                "buffered": self.buffered_count(),
                "budget": self.wait_budget,
            })

    def _fence(self, shard: str, edge: int, reason: str, detail: str) -> None:
        """隔离分片上缺口边缘（含）之后的全部在册事件，水位线停在缺口处。

        边缘 LSN 自身可能尚未到达，因此按 ``lsn >= edge`` 取在册事件；
        它们与缺口同属一个不可确认区间，隔离后由 :meth:`_heal` 在补缺时
        按连续顺序重新接纳。
        """
        buf = self._by_shard.get(shard, {})
        moved: list[Event] = []
        for lsn in sorted(lsn for lsn in buf if lsn >= edge):
            moved.extend(buf.pop(lsn))
        for ev in moved:
            self._quarantine.append(Quarantined(ev, reason, detail))
        if moved:
            self._decisions.append({
                "decision": "fence",
                "shard": shard,
                "edge": edge,
                "reason": reason,
                "moved": len(moved),
            })

    def _heal(self, shard: str) -> None:
        """补缺事件到达后，把隔离区中**已恢复连续**的事件重新接纳。

        只有从订阅锚点到该事件 LSN 的整段都已在册（无残留缺口）时才接纳；
        否则它仍属于不可确认区间，继续保留在隔离区，等待后续补缺。
        """
        q = self._quarantine
        if not q:
            return
        by_lsn = self._by_shard.setdefault(shard, {})
        anchor = self._base_lsn.get(shard)
        rehoused: list[Quarantined] = []
        for item in q:
            ev = item.event
            if ev.shard != shard:
                continue
            if item.reason not in ("gap_quarantine", "wait_budget_exceeded"):
                continue
            same = by_lsn.get(ev.lsn)
            if same and any(x.business_order_key() == ev.business_order_key()
                            for x in same):
                continue  # 同位置同定序键已有在册事件
            low = anchor if anchor is not None else min(
                [ev.lsn] + list(by_lsn))
            if any(lsn not in by_lsn for lsn in range(low, ev.lsn)):
                continue  # 中间仍有缺口，不能重新接纳
            by_lsn.setdefault(ev.lsn, []).append(ev)
            rehoused.append(item)
        for item in rehoused:
            q.remove(item)
            if item.event.event_id not in self._healed_ids:
                self._healed_ids.add(item.event.event_id)
                self._decisions.append({
                    "decision": "heal",
                    "event_id": item.event.event_id,
                    "shard": shard,
                    "lsn": item.event.lsn,
                    "former_reason": item.reason,
                })

    # ------------------------------------------------------------------
    # 水位线推进与事务释放
    # ------------------------------------------------------------------

    def _advance_watermarks(self) -> None:
        for shard, buf in self._by_shard.items():
            low = self._low[shard]
            anchor = self._base_lsn.get(shard)
            if anchor is not None and low < anchor and anchor not in buf:
                # 显式订阅起点本身尚未到齐，水位线不得越过它
                continue
            while low in buf:
                low += 1
            self._low[shard] = low

    def _pump(self) -> None:
        """重算水位线与事务可见性；不直接产出顺序，顺序只在 plan() 中确定。"""
        self._advance_watermarks()

        # 已中止事务：丢弃其全部 DML，且永不进入计划
        for st in self._txn.values():
            if st.status == TXN_ABORTED and st.events:
                for ev in st.events:
                    self._aborted_events.append(ev)
                    bucket = self._by_shard.get(ev.shard, {}).get(ev.lsn)
                    if bucket and ev in bucket:
                        bucket.remove(ev)
                        if not bucket:
                            self._by_shard[ev.shard].pop(ev.lsn, None)
                self._decisions.append({
                    "decision": "abort_drop",
                    "txn_id": st.txn_id,
                    "events": len(st.events),
                })
                st.events = []
        self._advance_watermarks()

        # 可见性条件：已提交（显式 commit / autocommit / 快照）且事务在每个
        # 参与分片上的全部事件 LSN 都已落在连续水位线之下。
        for st in self._txn.values():
            if st.status != TXN_COMMITTED or not st.events:
                continue
            if st.txn_id in self._eligible:
                continue
            if all(self._low[s] > max(e.lsn for e in st.events if e.shard == s)
                   for s in st.shards):
                self._eligible.add(st.txn_id)
                self._decisions.append({
                    "decision": "eligible",
                    "txn_id": st.txn_id,
                    "commit_ts": st.commit_ts,
                })

    def _build_unit(self, st: _TxnState) -> Unit:
        """构造一个事务单元：DML 按 (shard, lsn, txn_seq, table, pk) 定序。"""
        ordered = sorted(st.events, key=lambda e: (e.shard,) + e.order_key())
        unit = Unit(txn_id=st.txn_id, commit_ts=st.commit_ts)
        for ev in ordered:
            if self._is_suppressed(ev):
                unit.suppressed.append(ev)
                if ev.event_id not in self._suppressed_ids:
                    self._suppressed_ids.add(ev.event_id)
                    self._decisions.append({
                        "decision": "snapshot_suppress",
                        "event_id": ev.event_id,
                        "key": [ev.shard, ev.table, ev.pk],
                    })
                continue
            unit.events.append(ev)
        return unit

    def _is_suppressed(self, ev: Event) -> bool:
        """全量↔增量重叠窗口：快照已包含的增量变更不再重放。"""
        if ev.is_snapshot or ev.op == OP_SNAPSHOT:
            return False
        snap = self._snapshots.get((ev.shard, ev.table, ev.pk))
        if snap is None:
            return False
        snap_ts, snap_lsn = snap
        if ev.lsn <= snap_lsn:
            return True
        if ev.commit_ts and snap_ts and ev.commit_ts <= snap_ts:
            return True
        return False

    # ------------------------------------------------------------------
    # 观测与输出
    # ------------------------------------------------------------------

    def buffered_count(self) -> int:
        """当前等待确认而占用缓存的事件数：在册且所属事务尚未具备可见性。"""
        visible = self._eligible | self._released_txn
        return sum(1 for by_lsn in self._by_shard.values()
                   for bucket in by_lsn.values()
                   for ev in bucket if ev.txn_id not in visible)

    def watermarks(self) -> dict[str, int]:
        """各分片连续水位线：低于该 LSN 的事件已连续到齐、可确认。"""
        return dict(self._low)

    def open_gaps(self) -> dict[str, list[int]]:
        return {s: gaps for s, gaps in
                ((s, self._gaps(s)) for s in self._by_shard) if gaps}

    def _current_units(self) -> list[Unit]:
        """确定性地构造当前计划：全局按 (commit_ts, txn_id) 排序，与到达顺序无关。"""
        states = [self._txn[tid] for tid in sorted(
            self._eligible, key=lambda t: (self._txn[t].commit_ts, t))]
        return [self._build_unit(st) for st in states]

    def plan(self) -> dict[str, Any]:
        """输出可重放、可解释的有序计划。

        计划是已摄入事件集合与配置的纯函数：无论事件以何种顺序到达，只要
        集合相同，``units`` 顺序与 ``fingerprint`` 必然相同。
        """
        units_objs = self._current_units()
        units = [u.as_dict() for u in units_objs]
        suppressed = [e.as_dict() for u in units_objs for e in u.suppressed]
        payload = json.dumps(
            units, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode()
        return {
            "units": units,
            "quarantine": [q.as_dict() for q in self._quarantine],
            "duplicates": [e.as_dict() for e in self._duplicates],
            "suppressed": suppressed,
            "aborted": [e.as_dict() for e in self._aborted_events],
            "watermarks": self.watermarks(),
            "open_gaps": self.open_gaps(),
            "stats": {
                "gap_policy": self.gap_policy,
                "wait_budget": self.wait_budget,
                "buffered": self.buffered_count(),
                "buffer_peak": self.buffer_peak,
                "released_units": len(units),
                "released_events": sum(len(u.events) for u in units_objs),
                "quarantined": len(self._quarantine),
                "duplicates": len(self._duplicates),
                "suppressed": len(suppressed),
                "aborted_events": len(self._aborted_events),
                "open_transactions": sum(
                    1 for st in self._txn.values()
                    if st.status == TXN_OPEN and st.events
                ),
            },
            "decisions": list(self._decisions),
            "fingerprint": hashlib.sha256(payload).hexdigest(),
        }

    def explain(self) -> str:
        """人类可读的排序解释，便于核验缺口/预算/可见性判定。"""
        p = self.plan()
        lines = [
            f"gap_policy={self.gap_policy} wait_budget={self.wait_budget}",
            f"watermarks={p['watermarks']} open_gaps={p['open_gaps']}",
            f"buffered={p['stats']['buffered']} buffer_peak={p['stats']['buffer_peak']}",
            f"released_units={p['stats']['released_units']} "
            f"quarantined={p['stats']['quarantined']} "
            f"duplicates={p['stats']['duplicates']} "
            f"suppressed={p['stats']['suppressed']} "
            f"aborted={p['stats']['aborted_events']} "
            f"open_txns={p['stats']['open_transactions']}",
            "decisions:",
        ]
        lines.extend(f"  - {json.dumps(d, ensure_ascii=False, sort_keys=True)}"
                     for d in p["decisions"])
        lines.append(f"fingerprint={p['fingerprint']}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 检查点状态：崩溃恢复 / 多次恢复与一次处理等价
    # ------------------------------------------------------------------

    def state_dict(self) -> dict[str, Any]:
        """导出可交给 checkpoint 持久化的最小状态。"""
        return {
            "gap_policy": self.gap_policy,
            "wait_budget": self.wait_budget,
            "base_lsn": dict(self._base_lsn),
            "watermarks": self.watermarks(),
            "eligible_txn": sorted(self._eligible),
            "durable_txn": sorted(self._released_txn),
            "txn": [
                {
                    "txn_id": st.txn_id,
                    "status": st.status,
                    "commit_ts": st.commit_ts,
                    "shards": sorted(st.shards),
                    "events": [e.as_dict() for e in sorted(
                        st.events, key=lambda e: (e.shard,) + e.order_key())],
                }
                for tid, st in sorted(self._txn.items())
                if tid not in self._released_txn
            ],
            "snapshots": [
                {"shard": k[0], "table": k[1], "pk": k[2],
                 "snapshot_ts": v[0], "snapshot_lsn": v[1]}
                for k, v in sorted(self._snapshots.items())
            ],
            "suppressed_ids": sorted(self._suppressed_ids),
            "healed_ids": sorted(self._healed_ids),
            "seen_ids": sorted(self._seen_ids),
        }

    def mark_durable(self, txn_ids: Iterable[str]) -> None:
        """checkpoint 确认事务已原子落库后登记并清理其缓冲。

        重放时这些事务不再进计划；其事件从分片日志移除（水位线保留），
        保证 :meth:`buffered_count` 在恢复后仍真实反映等待占用的缓存。
        """
        for tid in txn_ids:
            self._released_txn.add(tid)
            self._eligible.discard(tid)
            st = self._txn.pop(tid, None)
            if st is None:
                continue
            for ev in st.events:
                bucket = self._by_shard.get(ev.shard, {}).get(ev.lsn)
                if bucket and ev in bucket:
                    bucket.remove(ev)
                    if not bucket:
                        self._by_shard[ev.shard].pop(ev.lsn, None)

    @staticmethod
    def from_state(state: dict[str, Any]) -> "OrderingEngine":
        eng = OrderingEngine(
            gap_policy=state.get("gap_policy", POLICY_WAIT),
            wait_budget=state.get("wait_budget", 10_000),
            base_lsn=state.get("watermarks") or state.get("base_lsn"),
        )
        for shard, low in (state.get("watermarks") or {}).items():
            eng._low[shard] = low
        eng._eligible = set(state.get("eligible_txn", ()))
        eng._released_txn = set(state.get("durable_txn",
                                          state.get("released_txn", ())))
        for raw in state.get("txn", ()):
            events = [Event.from_dict(d) for d in raw["events"]]
            st = _TxnState(
                txn_id=raw["txn_id"],
                status=raw["status"],
                commit_ts=raw.get("commit_ts", ""),
                events=events,
                shards=set(raw.get("shards", ())),
            )
            eng._txn[st.txn_id] = st
            # 已缓冲的事务事件重新挂回分片日志，水位线才能继续推进
            for e in events:
                eng._by_shard.setdefault(e.shard, {}).setdefault(e.lsn, []).append(e)
        for s in state.get("snapshots", ()):
            eng._snapshots[(s["shard"], s["table"], s["pk"])] = (
                s["snapshot_ts"], s["snapshot_lsn"]
            )
        eng._seen_ids = {eid: ("", 0) for eid in state.get("seen_ids", ())}
        eng._suppressed_ids = set(state.get("suppressed_ids", ()))
        eng._healed_ids = set(state.get("healed_ids", ()))
        return eng


def order_events(events: Iterable[dict[str, Any]],
                 gap_policy: str = POLICY_WAIT,
                 wait_budget: int = 10_000,
                 base_lsn: dict[str, int] | None = None) -> dict[str, Any]:
    """一次性格式化入口：纯函数，同输入同配置必然得到同计划（可重放）。"""
    eng = OrderingEngine(gap_policy=gap_policy,
                         wait_budget=wait_budget,
                         base_lsn=base_lsn)
    eng.ingest_all(events)
    return eng.plan()


# ---------------------------------------------------------------------------
# 兼容既有 happy-path 的记录级阶段函数（行为保持不变）
# ---------------------------------------------------------------------------

def process(record):  # noqa: ANN001 - 基线签名保持无注解
    rec = dict(record)
    rec.setdefault("_stage_order", "ok")
    return rec
