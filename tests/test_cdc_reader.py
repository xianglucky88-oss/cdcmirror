"""CDC 读取器离线故障注入测试。

覆盖：心跳 / 半个事务 / 断线重连 / 重复事件重放、位点只在下游确认后推进、
坏事件与位点失效的诊断分级、空批、大事务拆分的资源边界、并发与重入序列一致性。
所有连接均为脚本化替身，全程离线。
"""
import threading

import pytest

from src.cdcmirror import cdc_reader as cr
from src.cdcmirror.cdc_reader import (
    CDCReader, IdempotentMemorySink, MemoryCheckpoint, SqliteCheckpoint,
    TransientDisconnect, CheckpointUnavailable, BadEvent, BufferLimitExceeded,
    EVT_HEARTBEAT, EVT_BEGIN, EVT_DATA, EVT_COMMIT,
)


# --------------------------------------------------------------------------- #
# 脚本化离线连接替身
# --------------------------------------------------------------------------- #
class ScriptedConnection:
    """按脚本依次返回事件；可在指定 pull 次数注入断线。

    ``start_at`` 模拟真实连接的 resume 语义：重连只拿到从某个脚本下标
    开始的后缀（源端按最后确认位点裁剪），而不是全量回放。
    """

    def __init__(self, script, fail_at=None, fail_exc=None, start_at=0):
        self.script = list(script)
        self.idx = start_at
        self.pulls = 0
        self.fail_at = fail_at
        self.fail_exc = fail_exc or TransientDisconnect("simulated socket reset")
        self.closed = False

    def pull(self):
        call = self.pulls
        self.pulls += 1
        if self.fail_at is not None and call == self.fail_at:
            raise self.fail_exc
        if self.idx >= len(self.script):
            return None
        item = self.script[self.idx]
        self.idx += 1
        return item

    def close(self):
        self.closed = True


def hb():
    return {"type": EVT_HEARTBEAT}


def begin(txn, pos):
    return {"type": EVT_BEGIN, "txn": txn, "position": pos}


def data(pos, rid, **payload):
    return {"type": EVT_DATA, "position": pos,
            "record": {"id": rid, **payload}}


def commit(pos):
    return {"type": EVT_COMMIT, "position": pos}


def make_factory(script, fail_at=None, fail_exc=None, replay_start=0):
    """首次连接在 fail_at 处断线，重连只返回脚本 ``replay_start`` 之后的后缀。"""
    calls = {"n": 0}

    def factory():
        first = calls["n"] == 0
        calls["n"] += 1
        return ScriptedConnection(
            script,
            fail_at=fail_at if first else None,
            fail_exc=fail_exc,
            start_at=0 if first else replay_start)

    return factory, calls


# --------------------------------------------------------------------------- #
# 1. 心跳 → 半个事务 → 断线 → 重连重放（含重复/陈旧事件）
# --------------------------------------------------------------------------- #
def test_heartbeat_half_txn_disconnect_then_replay():
    script = [
        hb(),
        begin("txn1", 1), data(2, "e1", v=1), data(3, "e2", v=2), commit(4),
        begin("txn2", 4), data(5, "e3", v=3),   # 半个事务后断线（第 7 次 pull）
        # —— 重连后源端从已确认位点(4)重新给出：先重放未完成事务，
        #    再给新增事件；重放事件携带陈旧/重复位点 ——
        hb(),
        begin("txn2", 4),
        data(3, "e1", v=1),                     # 陈旧事件：位点 <= 已确认位点
        data(5, "e3", v=3), data(6, "e4", v=4), commit(7),
    ]
    factory, calls = make_factory(script, fail_at=7)
    sink = IdempotentMemorySink()
    cp = MemoryCheckpoint()
    reader = CDCReader(factory, sink, cp)

    result = reader.run()

    # 目标提交记录：顺序约束保持，去重后恰好四个事件
    assert sink.sequence() == ("e1", "e2", "e3", "e4")
    assert result.last_position == reader.position == 7
    assert reader.reconnects == 1 and result.reconnects == 1
    assert reader.heartbeats == 3
    # 重放的已确认 e1/e2 与陈旧 e1 均被位点去重，不产生重复投递
    assert reader.duplicates_seen == 3
    assert calls["n"] == 2                      # 确实重连了一次

    # 心跳绝不推进位点：位点推进只对应真实事务提交，
    # 且推进到 COMMIT 边界（4 而非最后 data 的 3）
    assert [a[0] for a in cp.advances] == [4, 7]

    # 用目标提交记录反查每次位点推进：推进时对应记录均已在下游确认
    for pos, ids, partial in cp.advances:
        assert partial is False
        for rid in ids:
            assert rid in sink.committed
    assert cp.advances[0][1] == ("e1", "e2")
    assert cp.advances[1][1] == ("e3", "e4")

    # 断线后未确认的半个事务缓冲被丢弃，重连后整体重放
    stats = reader.buffer_stats()
    assert stats["buffered_events"] == 0 and stats["buffered_bytes"] == 0
    assert stats["peak_events"] >= 2

    # 诊断区分暂时断线（可重试）
    diag = reader.diagnostics[-1]
    assert diag["kind"] == "TransientDisconnect" and diag["retryable"] is True
    reader.close()


# --------------------------------------------------------------------------- #
# 2. 下游已确认但位点写失败：重复投递可安全重放，顺序不被破坏
# --------------------------------------------------------------------------- #
def test_checkpoint_store_failure_replays_idempotently():
    # 重放事件携带更高的源位点（绕过读取器的位点去重），
    # 由下游幂等吸收——这才是完整的 at-least-once 重放链路。
    script = [begin("t", 1), data(2, "e1"), data(3, "e2"), commit(4),
              begin("t", 5), data(6, "e1"), data(7, "e2"), commit(8)]
    sink = IdempotentMemorySink()
    cp = MemoryCheckpoint()
    cp.fail_next_store(1)                        # 第一次位点写入瞬时失败
    # 重连只拿到未确认的后缀（源端按最后确认位点裁剪）
    reader = CDCReader(make_factory(script, replay_start=4)[0], sink, cp)

    reader.run()

    # 下游收到两次真实投递（at-least-once），但幂等吸收，顺序不变
    assert sink.all_committed_ids() == ("e1", "e2", "e1", "e2")
    assert sink.sequence() == ("e1", "e2")
    assert set(sink.committed) == {"e1", "e2"}
    # 失败的位点写入不留审计；重放确认后才推进到最终位点
    assert cp.advances == [(8, ("e1", "e2"), False)]
    assert reader.position == 8
    assert reader.delivered_total == 4
    # 位点失败触发一次连接重建重放，诊断阶段与 pull 断线区分
    assert reader.reconnects == 1
    assert reader.store_retries == 1
    assert reader.diagnostics[-1]["phase"] == "checkpoint"


# --------------------------------------------------------------------------- #
# 3. 坏事件：不可重试，读到即停，不推进位点
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("bad_event", [
    {"type": "mystery", "position": 1},         # 未知类型
    {"type": EVT_DATA, "position": 2, "record": {"id": "x"}},  # 事务外 data
    "not-a-dict",                               # 结构非法
    {"type": EVT_COMMIT, "position": 3},        # 无 begin 的 commit
])
def test_bad_event_is_non_retryable_and_stops(bad_event):
    sink = IdempotentMemorySink()
    cp = MemoryCheckpoint()
    reader = CDCReader(make_factory([bad_event])[0], sink, cp)

    with pytest.raises(BadEvent):
        reader.pump()

    assert reader.position is None              # 坏事件不推进位点
    assert sink.commit_calls == 0
    diag = reader.diagnostics[-1]
    assert diag["retryable"] is False
    assert diag["kind"] in {"BadEvent", "BufferLimitExceeded"}


def test_nested_begin_is_bad_event():
    script = [begin("a", 1), begin("b", 2)]
    reader = CDCReader(make_factory(script)[0],
                       IdempotentMemorySink(), MemoryCheckpoint())
    with pytest.raises(BadEvent):
        reader.pump()


# --------------------------------------------------------------------------- #
# 4. 位点失效：不可重试，不做重连尝试
# --------------------------------------------------------------------------- #
def test_checkpoint_unavailable_is_fatal():
    factory, calls = make_factory(
        [hb()], fail_at=0,
        fail_exc=CheckpointUnavailable("binlog position expired"))
    reader = CDCReader(factory, IdempotentMemorySink(), MemoryCheckpoint())

    with pytest.raises(CheckpointUnavailable):
        reader.pump()

    assert reader.reconnects == 0               # 失效位点不重试
    assert calls["n"] == 1
    assert reader.state == "stopped"
    diag = reader.diagnostics[-1]
    assert diag["kind"] == "CheckpointUnavailable"
    assert diag["retryable"] is False


def test_reconnect_budget_exhausted_raises_transient():
    # 每次连接的第一次 pull 都断线
    def factory():
        return ScriptedConnection([hb()], fail_at=0)

    reader = CDCReader(factory, IdempotentMemorySink(),
                       MemoryCheckpoint(), max_reconnects=2)
    with pytest.raises(TransientDisconnect):
        reader.pump()
    assert reader.reconnects == 3               # 首次 + 2 次重连后放弃


# --------------------------------------------------------------------------- #
# 5. 心跳与空批：不出数、不推进位点
# --------------------------------------------------------------------------- #
def test_heartbeat_and_empty_batch_never_advance_checkpoint():
    script = [hb(), [], hb(), []]
    sink = IdempotentMemorySink()
    cp = MemoryCheckpoint()
    reader = CDCReader(make_factory(script)[0], sink, cp)

    result = reader.run()

    assert result.delivered == []
    assert reader.heartbeats == 2
    assert reader.empty_batches == 2
    assert reader.position is None
    assert cp.advances == []
    assert sink.deliver_calls == 0


# --------------------------------------------------------------------------- #
# 6. 大事务拆分：可测量的资源边界，子批先确认后推进位点
# --------------------------------------------------------------------------- #
def test_large_transaction_split_by_event_count():
    script = [begin("big", 1)]
    for i in range(5):
        script.append(data(2 + i, f"r{i}"))
    script.append(commit(7))

    sink = IdempotentMemorySink()
    cp = MemoryCheckpoint()
    reader = CDCReader(make_factory(script)[0], sink, cp,
                       max_buffer_events=2, max_buffer_bytes=10_000_000)

    reader.run()

    # 5 条记录按 2/2/1 拆成三个确认批；末批推进到 COMMIT 边界 7
    assert sink.sequence() == tuple(f"r{i}" for i in range(5))
    assert sink.commit_calls == 3
    assert [a[0] for a in cp.advances] == [3, 5, 7]
    # 中段位点显式标记事务未完成，供重连判定整体重放
    assert [a[2] for a in cp.advances] == [True, True, False]
    stats = reader.buffer_stats()
    assert stats["peak_events"] == 2            # 缓冲从未越界
    assert stats["buffered_events"] == 0
    # 反查：每次推进的记录都已在下游
    for _pos, ids, _partial in cp.advances:
        assert all(rid in sink.committed for rid in ids)


def test_large_transaction_split_by_bytes():
    script = [begin("big", 1),
              data(2, "a", payload="x" * 50),
              data(3, "b", payload="y" * 50),
              commit(4)]
    one = cr._event_size(script[1])
    sink = IdempotentMemorySink()
    reader = CDCReader(make_factory(script)[0], sink, MemoryCheckpoint(),
                       max_buffer_events=100, max_buffer_bytes=one + 5)

    reader.run()

    assert sink.sequence() == ("a", "b")
    assert sink.commit_calls == 2               # 字节边界触发逐条冲刷
    assert reader.buffer_stats()["peak_bytes"] <= one + 5


def test_single_oversized_event_is_fatal():
    huge = data(2, "big", payload="z" * 500)
    script = [begin("t", 1), huge, commit(3)]
    reader = CDCReader(make_factory(script)[0],
                       IdempotentMemorySink(), MemoryCheckpoint(),
                       max_buffer_events=100, max_buffer_bytes=64)
    with pytest.raises(BufferLimitExceeded):
        reader.run()
    assert reader.diagnostics[-1]["retryable"] is False


# --------------------------------------------------------------------------- #
# 7. 并发与重入：所有调用方看到同一事件序列
# --------------------------------------------------------------------------- #
def test_concurrent_pumps_produce_one_consistent_sequence():
    script = []
    expected = []
    pos = 1
    for t in range(3):
        script.append(begin(f"t{t}", pos))
        pos += 1
        for k in range(3):
            rid = f"t{t}-r{k}"
            script.append(data(pos, rid))
            expected.append(rid)
            pos += 1
        script.append(commit(pos))
        pos += 1

    reader = CDCReader(make_factory(script)[0],
                       IdempotentMemorySink(), MemoryCheckpoint(),
                       max_buffer_events=4)
    errors = []

    def worker():
        try:
            for _ in range(50):
                r = reader.pump(max_events=3)
                if r.eos:
                    break
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert errors == []
    assert reader.confirmed_sequence() == tuple(expected)
    # 无重复、无乱序
    assert len(set(reader.confirmed_sequence())) == len(expected)


def test_reentrant_pump_returns_snapshot_without_double_delivery():
    script = [begin("t", 1), data(2, "e1"), data(3, "e2"), commit(4)]
    sink = IdempotentMemorySink()
    reader = CDCReader(make_factory(script)[0], sink, MemoryCheckpoint())
    seen = {}

    original_commit = sink.commit

    def commit_with_reentrancy():
        original_commit()
        # 在下游确认过程中重入读取器
        seen["reentrant"] = reader.pump()

    sink.commit = commit_with_reentrancy
    reader.run()

    snap = seen["reentrant"]
    assert snap.reentrant is True
    assert [r["id"] for r in snap.delivered] == ["e1", "e2"]
    assert sink.all_committed_ids() == ("e1", "e2")   # 重入没有造成二次投递


# --------------------------------------------------------------------------- #
# 8. 兼容性与持久化位点
# --------------------------------------------------------------------------- #
def test_legacy_process_contract_unchanged():
    src = {"id": "r-001", "value": 10}
    out = cr.process(src)
    assert out == {"id": "r-001", "value": 10, "_stage_cdc_reader": "naive"}
    assert src == {"id": "r-001", "value": 10}      # 输入不被修改
    assert cr.process({"_stage_cdc_reader": "kept"})["_stage_cdc_reader"] == "kept"


def test_pipeline_happy_path_still_runs():
    from src.cdcmirror import pipeline
    result = pipeline.run()
    assert result["fed"] == 4 and result["rows"] == 3


def test_sqlite_checkpoint_roundtrip():
    cp = SqliteCheckpoint(":memory:")
    assert cp.load_state() == (None, None)
    cp.store(9, delivered_ids=("x",), open_txn=42)
    assert cp.load_state() == (9, 42)
    cp.store(10, delivered_ids=("y",))
    assert cp.load_state() == (10, None)
    cp.close()


def test_memory_checkpoint_injected_failure_is_transient():
    cp = MemoryCheckpoint()
    cp.fail_next_store(1)
    with pytest.raises(TransientDisconnect):
        cp.store(1)
    cp.store(2)
    assert cp.position() == 2
