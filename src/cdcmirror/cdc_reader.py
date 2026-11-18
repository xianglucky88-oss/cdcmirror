"""CDC 读取器：长连接批次语义 + 事务边界 + 位点只在下游确认后推进。

职责边界
--------
本模块只负责「从源连接拉 CDC 事件 → 按事务拼批 → 交给下游（sink）→
下游确认后才推进位点」。它不做字段规范化（normalizer 的职责），也不做
最终落库（store/pipeline 的职责）。

关键保证
~~~~~~~~
1. 位点（checkpoint）只在 ``sink.commit()`` 成功之后写入；心跳/空批不推进位点。
2. 一个事务的记录按源顺序整体投递；大事务在 ``max_buffer_events`` /
   ``max_buffer_bytes`` 边界内拆成子批，子批同样是「先确认后位点」。
3. 断线为可重试故障（:class:`TransientDisconnect`），重连后从最后确认位点继续，
   未确认事务整体重放——要求下游幂等。
4. 位点失效与坏事件为不可重试故障（:class:`CheckpointUnavailable` /
   :class:`BadEvent`），读到即停，不推进位点，调用方应停读而非重试。
5. 资源边界可测量：``buffer_stats()`` 暴露当前缓冲事件数/字节数/水位线。
6. ``pump`` 串行化：并发/重入调用拿到的是同一个已确认事件序列，不会重复出数。

兼容性
~~~~~~
基线流水线调用的模块级 :func:`process` 保持原样（复制记录并补阶段标记），
``make run`` / 既有 happy-path 测试不受影响。
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

__all__ = [
    # 基线兼容入口
    "process",
    # 事件协议常量
    "EVT_HEARTBEAT", "EVT_BEGIN", "EVT_DATA", "EVT_COMMIT",
    # 诊断异常
    "CDCError", "TransientDisconnect", "CheckpointUnavailable",
    "BadEvent", "BufferLimitExceeded",
    # 数据结构
    "PumpResult", "MemoryCheckpoint", "SqliteCheckpoint",
    "IdempotentMemorySink", "CDCReader",
]


# --------------------------------------------------------------------------- #
# 事件协议（纯 dict，离线测试与真实适配器都可构造）
# --------------------------------------------------------------------------- #
EVT_HEARTBEAT = "heartbeat"   # 连接保活：不投递、不推进位点
EVT_BEGIN = "begin"           # 事务开始
EVT_DATA = "data"             # 事务内数据记录，必须位于 begin..commit 之间
EVT_COMMIT = "commit"         # 事务提交：触发整事务（或子批）投递
# pull() 返回 None 表示源端干净结束（EOS）

# --------------------------------------------------------------------------- #
# 诊断异常：retryable 区分「重试还是停读」
# --------------------------------------------------------------------------- #
class CDCError(Exception):
    """所有 CDC 读取故障的基类。``retryable=True`` 可重连重试。"""

    retryable = False

    def __init__(self, message: str, *, position: Any = None):
        super().__init__(message)
        self.position = position


class TransientDisconnect(CDCError):
    """暂时断线（pull 失败/连接重置/位点存储暂时不可写）：可重连重试。"""

    retryable = True


class CheckpointUnavailable(CDCError):
    """位点失效：保存的位点源端已不认（binlog 过期等），不可重试，应停读。"""

    retryable = False


class BadEvent(CDCError):
    """坏事件：结构非法、顺序错乱或单事件超出资源上限。不可重试，应停读。"""

    retryable = False


class BufferLimitExceeded(BadEvent):
    """单个事务（或单个事件）超过配置的缓冲资源边界。"""

    retryable = False


# --------------------------------------------------------------------------- #
# 结果与工具
# --------------------------------------------------------------------------- #
@dataclass
class PumpResult:
    """一次 :meth:`CDCReader.pump` 的可观测结果。"""

    delivered: list[dict[str, Any]] = field(default_factory=list)
    heartbeats: int = 0
    empty_batches: int = 0
    reconnects: int = 0
    eos: bool = False
    reentrant: bool = False          # 重入/并发调用：未重新拉取，只回传序列快照
    last_position: Any = None        # 当前已确认位点（未确认过则为 None）

    @property
    def count(self) -> int:
        return len(self.delivered)


def _event_size(event: dict[str, Any]) -> int:
    """事件的确定性内存占用估计（字节），用于可测量的资源边界。"""
    return len(json.dumps(event, sort_keys=True, ensure_ascii=False,
                          default=str).encode("utf-8"))


# --------------------------------------------------------------------------- #
# 位点存储
# --------------------------------------------------------------------------- #
class MemoryCheckpoint:
    """进程内位点存储（线程安全），并保留推进审计轨迹供反查。

    ``fail_next_store`` 可注入一次瞬时写入失败，用于离线故障注入：
    写入失败时已投递数据在下游已确认，重连后会重复投递，由幂等 sink 吸收。
    """

    def __init__(self, position: Any = None):
        self._lock = threading.Lock()
        self._position = position
        self._open_txn: Any = None
        self._fail_next = 0
        # (新位点, 当时已确认事件 id 序列, 是否事务中段)
        self.advances: list[tuple[Any, tuple[str, ...], bool]] = []

    def load(self) -> Any:
        with self._lock:
            return self._position

    def load_state(self) -> tuple[Any, Any]:
        """返回 (已确认位点, 未完成事务起点)。重连后未完成事务必须整体重放。"""
        with self._lock:
            return self._position, self._open_txn

    def store(self, position: Any, *, delivered_ids: tuple[str, ...] = (),
              open_txn: Any = None) -> None:
        with self._lock:
            if self._fail_next > 0:
                self._fail_next -= 1
                raise TransientDisconnect("injected checkpoint store failure",
                                          position=position)
            self._position = position
            self._open_txn = open_txn
            self.advances.append((position, tuple(delivered_ids),
                                  open_txn is not None))

    def fail_next_store(self, times: int = 1) -> None:
        with self._lock:
            self._fail_next += times

    def position(self) -> Any:
        return self.load()


class SqliteCheckpoint:
    """sqlite 持久化位点（与 store.py 的本地 sqlite 技术栈一致）。"""

    def __init__(self, path: str = ":memory:"):
        import sqlite3
        self._lock = threading.Lock()
        self._con = sqlite3.connect(path, check_same_thread=False)
        self._con.execute(
            "CREATE TABLE IF NOT EXISTS cdc_checkpoint("
            "name TEXT PRIMARY KEY, position TEXT NOT NULL, open_txn TEXT)")
        self._con.commit()

    @staticmethod
    def _dump(value: Any) -> Optional[str]:
        return None if value is None else json.dumps(value, ensure_ascii=False)

    @staticmethod
    def _load(value: Optional[str]) -> Any:
        return None if value is None else json.loads(value)

    def load(self) -> Any:
        with self._lock:
            row = self._con.execute(
                "SELECT position FROM cdc_checkpoint WHERE name='default'").fetchone()
            return self._load(row[0]) if row else None

    def load_state(self) -> tuple[Any, Any]:
        with self._lock:
            row = self._con.execute(
                "SELECT position, open_txn FROM cdc_checkpoint WHERE name='default'"
            ).fetchone()
            if row is None:
                return None, None
            return self._load(row[0]), self._load(row[1])

    def store(self, position: Any, *, delivered_ids: tuple[str, ...] = (),
              open_txn: Any = None) -> None:
        with self._lock:
            self._con.execute(
                "INSERT INTO cdc_checkpoint(name,position,open_txn) "
                "VALUES('default',?,?) "
                "ON CONFLICT(name) DO UPDATE SET position=excluded.position, "
                "open_txn=excluded.open_txn",
                (self._dump(position), self._dump(open_txn)))
            self._con.commit()

    def close(self) -> None:
        with self._lock:
            self._con.close()


# --------------------------------------------------------------------------- #
# 下游：幂等内存 sink（测试替身/参考实现）
# --------------------------------------------------------------------------- #
class IdempotentMemorySink:
    """按记录 id 幂等接收的下游。

    真实下游应实现同样的协议：``deliver(records)`` 缓冲/写入，
    ``commit()`` 做持久确认；只有 ``commit()`` 返回后读取器才推进位点。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._pending: list[dict[str, Any]] = []
        self.committed: dict[str, dict[str, Any]] = {}
        # 全局提交序列：同 id 重复投递只计一次，但保留重复投递事实
        self.commit_sequence: list[str] = []
        self.deliver_calls = 0
        self.commit_calls = 0

    def deliver(self, records: list[dict[str, Any]]) -> None:
        with self._lock:
            self._pending.extend(records)
            self.deliver_calls += 1

    def commit(self) -> None:
        with self._lock:
            for rec in self._pending:
                rid = str(rec.get("id"))
                self.commit_sequence.append(rid)
                self.committed[rid] = rec
            self._pending = []
            self.commit_calls += 1

    def rollback(self) -> None:
        with self._lock:
            self._pending = []

    def sequence(self) -> tuple[str, ...]:
        """去重后的目标提交序列（顺序约束的验证基准）。"""
        with self._lock:
            seen: set[str] = set()
            ordered: list[str] = []
            for rid in self.commit_sequence:
                if rid not in seen:
                    seen.add(rid)
                    ordered.append(rid)
            return tuple(ordered)

    def all_committed_ids(self) -> tuple[str, ...]:
        """含重复投递的完整提交序列（验证 at-least-once 重放）。"""
        with self._lock:
            return tuple(self.commit_sequence)


# --------------------------------------------------------------------------- #
# 读取器主体
# --------------------------------------------------------------------------- #
class CDCReader:
    """长连接 CDC 读取器。

    参数
    ~~~~
    connect:
        无参可调用，返回一个连接对象。连接须实现 ``pull()``（返回事件 dict、
        事件列表或 ``None``=EOS）与 ``close()``。断线由连接抛出
        :class:`TransientDisconnect`，位点失效抛出 :class:`CheckpointUnavailable`。
    sink:
        下游，实现 ``deliver(list)`` / ``commit()`` / ``rollback()``。
    checkpoint:
        位点存储，实现 ``load_state()`` / ``store(position, ...)``。
    max_buffer_events / max_buffer_bytes:
        大事务缓冲的可测量资源边界。单事件超限直接 :class:`BufferLimitExceeded`；
        事务累积触界则先把已缓冲部分作为子批「确认 + 推进位点」。
    max_reconnects:
        单次 :meth:`pump` 内连续断线的重连上限。
    """

    def __init__(self,
                 connect: Callable[[], Any],
                 sink: IdempotentMemorySink,
                 checkpoint: MemoryCheckpoint,
                 *,
                 max_buffer_events: int = 1000,
                 max_buffer_bytes: int = 8 * 1024 * 1024,
                 max_reconnects: int = 5):
        if max_buffer_events < 1 or max_buffer_bytes < 1:
            raise ValueError("buffer limits must be positive")
        self._connect = connect
        self._sink = sink
        self._checkpoint = checkpoint
        self._max_events = max_buffer_events
        self._max_bytes = max_buffer_bytes
        self._max_reconnects = max_reconnects

        self._lock = threading.RLock()
        self._con: Any = None
        self._in_pump = False                 # 重入/并发守卫

        # 事务拼装状态
        self._txn_id: Any = None
        self._txn_buffer: list[dict[str, Any]] = []
        self._txn_bytes = 0
        self._txn_peak_events = 0
        self._txn_peak_bytes = 0

        # 已确认位点与已确认事件序列（重入快照的基准）
        self._position: Any = None
        self._confirmed_ids: list[str] = []

        # 可观测性
        self.heartbeats = 0
        self.empty_batches = 0
        self.reconnects = 0          # 真实连接重建次数（pull 断线后）
        self.store_retries = 0      # 下游已确认但位点写入失败的重试次数
        self.delivered_total = 0
        self.duplicates_seen = 0
        self.diagnostics: list[dict[str, Any]] = []

    # -- 基础属性 ----------------------------------------------------------- #
    @property
    def position(self) -> Any:
        with self._lock:
            return self._position

    @property
    def state(self) -> str:
        """连接恢复状态：idle / connected / reconnecting / stopped。

        - idle：尚未建立连接且无故障；
        - connected：连接存活；
        - reconnecting：连接存活但本轮发生过重连（瞬时故障恢复中）；
        - stopped：发生不可重试故障（坏事件/位点失效），调用方应停读。
        """
        with self._lock:
            fatal = any(not d["retryable"] for d in self.diagnostics)
            if self._con is None:
                return "stopped" if fatal else "idle"
            return "reconnecting" if self.reconnects else "connected"

    def buffer_stats(self) -> dict[str, int]:
        """当前事务缓冲的资源占用与历史水位线（可测量的资源边界）。"""
        with self._lock:
            return {
                "buffered_events": len(self._txn_buffer),
                "buffered_bytes": self._txn_bytes,
                "peak_events": self._txn_peak_events,
                "peak_bytes": self._txn_peak_bytes,
                "max_events": self._max_events,
                "max_bytes": self._max_bytes,
            }

    def confirmed_sequence(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._confirmed_ids)

    def _record_diagnostic(self, exc: Exception, phase: str) -> None:
        kind = type(exc).__name__
        retryable = bool(getattr(exc, "retryable", False))
        self.diagnostics.append({
            "kind": kind,
            "retryable": retryable,
            "phase": phase,
            "message": str(exc),
            "position": getattr(exc, "position", None),
        })

    # -- 连接管理 ----------------------------------------------------------- #
    def _open_connection(self) -> Any:
        try:
            con = self._connect()
        except CheckpointUnavailable:
            raise
        except TransientDisconnect:
            raise
        except Exception as exc:  # 适配器把底层故障包装成 TransientDisconnect 更佳
            raise TransientDisconnect(f"connect failed: {exc}") from exc
        if con is None or not hasattr(con, "pull"):
            raise TransientDisconnect("connection factory returned no usable connection")
        return con

    def _ensure_connection(self) -> Any:
        if self._con is None:
            self._con = self._open_connection()
            pos, _open_txn = self._checkpoint.load_state()
            # 重连后位点保持最后确认位置；未完成事务不做断点续传，
            # 缓冲清空，由源端从该位点重新给出事件整体重放。
            self._position = pos
            self._txn_id = None
            self._txn_buffer = []
            self._txn_bytes = 0
        return self._con

    def _drop_connection(self) -> None:
        if self._con is not None:
            try:
                self._con.close()
            except Exception:
                pass
            self._con = None
        # 断线时未确认事务必须丢弃缓冲，重连后整体重放
        self._txn_id = None
        self._txn_buffer = []
        self._txn_bytes = 0

    # -- 事件校验 ----------------------------------------------------------- #
    @staticmethod
    def _validate_event(event: Any) -> dict[str, Any]:
        if not isinstance(event, dict) or "type" not in event:
            raise BadEvent(f"event must be a dict with 'type': {event!r}")
        etype = event["type"]
        if etype not in (EVT_HEARTBEAT, EVT_BEGIN, EVT_DATA, EVT_COMMIT):
            raise BadEvent(f"unknown event type {etype!r}",
                           position=event.get("position"))
        if etype != EVT_HEARTBEAT and "position" not in event:
            raise BadEvent(f"{etype} event missing position",
                           position=event.get("position"))
        return event

    # -- 事务缓冲与大事务拆分 ----------------------------------------------- #
    def _append_data(self, event: dict[str, Any]) -> None:
        """缓冲一条 data；触界时先冲刷子批，保证缓冲永不超过配置上限。"""
        size = _event_size(event)
        if size > self._max_bytes:
            raise BufferLimitExceeded(
                f"single event {size}B exceeds max_buffer_bytes={self._max_bytes}",
                position=event.get("position"))
        # 先判断边界再入缓冲：大事务在资源边界内拆成可确认的子批
        if self._txn_buffer and (
                len(self._txn_buffer) >= self._max_events
                or self._txn_bytes + size > self._max_bytes):
            self._flush(partial=True,
                        commit_position=self._txn_buffer[-1]["position"])
        self._txn_buffer.append(event)
        self._txn_bytes += size
        self._txn_peak_events = max(self._txn_peak_events, len(self._txn_buffer))
        self._txn_peak_bytes = max(self._txn_peak_bytes, self._txn_bytes)

    def _flush(self, *, partial: bool, commit_position: Any) -> Any:
        """把缓冲作为一个批次交给下游；下游确认后才推进位点。

        ``commit_position`` 是本批的位点上界：完整事务取 COMMIT 事件位点，
        大事务中段拆分取缓冲内最后一条 data 的位点（配合 ``open_txn`` 标记）。
        """
        if not self._txn_buffer:
            return self._position
        batch = self._txn_buffer
        last_position = commit_position
        records = [e["record"] for e in batch]
        open_txn = self._txn_id if partial else None

        # 1) 下游接收并确认——位点推进的唯一许可
        self._sink.deliver(records)
        self._sink.commit()

        # 下游确认即计入投递（at-least-once：位点失败重放会再次确认，
        # 由下游幂等吸收）
        ids = tuple(str(r.get("id")) for r in records)
        self.delivered_total += len(records)

        # 2) 确认之后才写位点；中段拆分记录 open_txn，保证重连可判定重放
        # 先清空拼装缓冲：无论位点写入成败，这批事件都已离开缓冲；
        # 若写入失败，重连后由源端从旧位点整体重放（下游幂等吸收）。
        self._txn_buffer = []
        self._txn_bytes = 0
        try:
            self._checkpoint.store(last_position,
                                   delivered_ids=ids,
                                   open_txn=open_txn)
        except TransientDisconnect:
            raise
        # 内存确认序列只在位点成功写入后更新，避免重放事件被错误去重
        self._position = last_position
        self._confirmed_ids.extend(ids)
        return last_position

    # -- 事件处理 ----------------------------------------------------------- #
    def _handle_event(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        event = self._validate_event(event)
        etype = event["type"]

        if etype == EVT_HEARTBEAT:
            self.heartbeats += 1
            return []

        pos = event["position"]
        # 重连重放时只有 data 需要按位点去重；begin/commit 是事务边界，
        # 即使位点已确认也必须照常处理，否则重放的事务无法重新拼装。
        if etype == EVT_DATA and self._position is not None and \
                pos is not None and pos <= self._position:
            self.duplicates_seen += 1
            return []

        if etype == EVT_BEGIN:
            if self._txn_id is not None and self._txn_id != event.get("txn"):
                raise BadEvent(
                    f"nested/overlapping txn {event.get('txn')!r} while "
                    f"{self._txn_id!r} open", position=pos)
            # 同 id 的 BEGIN 是重放信号（断线重连，或跨 pump 的开放事务被
            # 源端重新给出）：丢弃未确认缓冲，从事务起点重新拼装。
            self._txn_id = event.get("txn")
            self._txn_buffer = []
            self._txn_bytes = 0
            return []

        if etype == EVT_DATA:
            if self._txn_id is None:
                raise BadEvent("data event outside transaction", position=pos)
            if not isinstance(event.get("record"), dict):
                raise BadEvent("data event missing record payload", position=pos)
            self._append_data(event)
            return []

        # commit：事务边界，整事务（或最后一个子批）投递
        if self._txn_id is None:
            raise BadEvent("commit without begin", position=pos)
        if not self._txn_buffer:
            # 空事务：仅推进事务边界，不产生下游确认
            self._txn_id = None
            return []
        delivered = list(self._txn_buffer)
        # 位点推进到 COMMIT 边界，保证重放不会重复或遗漏整个事务
        self._flush(partial=False, commit_position=pos)
        self._txn_id = None
        return [e["record"] for e in delivered]

    # -- 拉取主循环 --------------------------------------------------------- #
    def pump(self, max_events: int = 100) -> PumpResult:
        """拉取并处理至多 ``max_events`` 个事件（心跳计入预算但不出数）。

        并发/重入安全：同一时刻只有一个调用真正拉取；其余调用立即返回
        当前已确认序列的快照（``reentrant=True``），保证所有调用方看到
        同一事件序列，绝不因重入而重复投递。
        """
        with self._lock:
            if self._in_pump:
                # 快照直接取自下游已确认记录：即使在 sink.commit() 回调内重入，
                # 也能反映刚确认的批次，且绝不触发第二次投递。
                snapshot = [dict(self._sink.committed[i])
                            for i in self._sink.sequence()]
                return PumpResult(
                    delivered=snapshot,
                    heartbeats=0, empty_batches=0, reconnects=0,
                    eos=False, reentrant=True, last_position=self._position)
            self._in_pump = True
            try:
                return self._pump_locked(max_events)
            finally:
                self._in_pump = False

    def _pump_locked(self, max_events: int) -> PumpResult:
        result = PumpResult()
        reconnect_attempts = 0
        processed = 0

        while processed < max_events:
            try:
                con = self._ensure_connection()
                raw = con.pull()
            except CheckpointUnavailable as exc:
                self._record_diagnostic(exc, "pull")
                self._drop_connection()
                raise
            except TransientDisconnect as exc:
                self._record_diagnostic(exc, "pull")
                self._drop_connection()
                reconnect_attempts += 1
                result.reconnects += 1
                self.reconnects += 1
                if reconnect_attempts > self._max_reconnects:
                    raise TransientDisconnect(
                        f"max reconnects {self._max_reconnects} exceeded") from exc
                continue  # 重连：从最后确认位点继续，未确认事务整体重放
            except Exception as exc:
                wrapped = TransientDisconnect(f"pull failed: {exc}")
                self._record_diagnostic(wrapped, "pull")
                self._drop_connection()
                reconnect_attempts += 1
                result.reconnects += 1
                self.reconnects += 1
                if reconnect_attempts > self._max_reconnects:
                    raise wrapped from exc
                continue

            reconnect_attempts = 0  # 一次成功拉取后重置连续断线计数

            if raw is None:
                result.eos = True
                break

            # 一次 pull 可返回单事件或一批事件；空列表即空批
            events = raw if isinstance(raw, list) else [raw]
            if not events:
                self.empty_batches += 1
                result.empty_batches += 1
                processed += 1
                continue

            for event in events:
                try:
                    delivered = self._handle_event(event)
                except TransientDisconnect as exc:
                    # 下游已确认但位点暂时写不进去：按可重试故障处理，
                    # 重建连接后由源端从最后确认位点重放（下游幂等吸收重复）。
                    # 与 pull 断线区分：诊断阶段标记为 checkpoint，并计 store_retries。
                    self._record_diagnostic(exc, "checkpoint")
                    self._drop_connection()
                    self.store_retries += 1
                    reconnect_attempts += 1
                    result.reconnects += 1
                    self.reconnects += 1
                    if reconnect_attempts > self._max_reconnects:
                        raise
                    break
                except BadEvent as exc:
                    self._record_diagnostic(exc, "handle")
                    raise
                result.delivered.extend(delivered)
                processed += 1
                if processed >= max_events:
                    break

        result.heartbeats = self.heartbeats
        result.last_position = self._position
        return result

    def run(self, *, max_pulls: int = 10_000) -> PumpResult:
        """持续拉取到 EOS 或不可重试故障。累计各次 pump 的统计。"""
        total = PumpResult()
        for _ in range(max_pulls):
            r = self.pump()
            total.delivered.extend(r.delivered)
            total.heartbeats = r.heartbeats
            total.empty_batches = r.empty_batches
            total.reconnects = r.reconnects
            if r.eos:
                total.eos = True
                break
        else:
            raise TransientDisconnect("run() exceeded max_pulls budget")
        total.last_position = self._position
        return total

    def close(self) -> None:
        with self._lock:
            self._drop_connection()


# --------------------------------------------------------------------------- #
# 基线兼容入口：旧 process() 调用与既有成功输入保持行为不变
# --------------------------------------------------------------------------- #
def process(record):
    """流水线阶段函数（基线行为，勿改语义）：复制记录并补阶段标记。"""
    rec = dict(record)
    rec.setdefault("_stage_cdc_reader", "naive")
    return rec
