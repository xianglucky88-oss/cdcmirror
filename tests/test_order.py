"""order/checkpoint/store 联合测试：排序、事务可见性、缺口、快照重叠、可重放与原子性。"""
import json
import sqlite3

import pytest

from src.cdcmirror import checkpoint, order, pipeline, store
from src.cdcmirror.order import OrderingEngine, order_events


# ---------------------------------------------------------------------------
# 构造工具
# ---------------------------------------------------------------------------

def ev(event_id, shard, lsn, op="insert", txn_id=None, txn_seq=0,
       commit_ts="", table="t", pk=None, is_snapshot=False,
       snapshot_ts=None, autocommit=False, data=None):
    return {
        "event_id": event_id, "shard": shard, "lsn": lsn, "op": op,
        "txn_id": txn_id, "txn_seq": txn_seq, "commit_ts": commit_ts,
        "table": table, "pk": pk if pk is not None else event_id,
        "is_snapshot": is_snapshot, "snapshot_ts": snapshot_ts,
        "autocommit": autocommit, "data": data or {},
    }


def commit(event_id, shard, lsn, txn_id, commit_ts):
    return ev(event_id, shard, lsn, op="commit", txn_id=txn_id,
              commit_ts=commit_ts, table="", pk="")


def make_con(tmp_path, name="cdc.db"):
    path = tmp_path if str(tmp_path).endswith(".db") else tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    store.ensure_cdc_tables(con)
    return con


def mirror_rows(con):
    return store.mirror_snapshot(con)


# ---------------------------------------------------------------------------
# 1. 同事务删除+插入按到达时间重排，最终镜像状态必须正确
# ---------------------------------------------------------------------------

def test_same_txn_delete_insert_arrival_reorder(tmp_path):
    # 真实顺序：T1 在 s1 上先 delete k1(lsn10) 再 insert k1(lsn11)；
    # 到达顺序被重排成 insert 先到、delete 后到。
    events = [
        ev("e-ins", "s1", 11, "insert", "T1", 2, "2026-11-01T00:01:00Z", pk="k1"),
        ev("e-del", "s1", 10, "delete", "T1", 1, "2026-11-01T00:01:00Z", pk="k1"),
        commit("c1", "s1", 12, "T1", "2026-11-01T00:01:00Z"),
    ]
    plan = order_events(events)
    assert len(plan["units"]) == 1
    ops = [e["op"] for e in plan["units"][0]["events"]]
    assert ops == ["delete", "insert"]  # LSN 定序，不按到达时间

    con = make_con(tmp_path)
    checkpoint.commit_plan(con, plan)
    rows = mirror_rows(con)
    assert set(rows) == {"s1:t:k1"}  # 删除被插入覆盖，行存在
    con.close()


def test_reverse_arrival_delete_last_wins_when_lsn_later(tmp_path):
    # 真实顺序 insert(lsn10) → delete(lsn11)，到达顺序相反：最终行必须消失
    events = [
        ev("e-del", "s1", 11, "delete", "T1", 2, "2026-11-01T00:01:00Z", pk="k1"),
        ev("e-ins", "s1", 10, "insert", "T1", 1, "2026-11-01T00:01:00Z", pk="k1"),
        commit("c1", "s1", 12, "T1", "2026-11-01T00:01:00Z"),
    ]
    plan = order_events(events)
    ops = [e["op"] for e in plan["units"][0]["events"]]
    assert ops == ["insert", "delete"]
    con = make_con(tmp_path)
    checkpoint.commit_plan(con, plan)
    assert mirror_rows(con) == {}
    con.close()


# ---------------------------------------------------------------------------
# 2. 跨分片乱序与事务整体可见性
# ---------------------------------------------------------------------------

def test_cross_shard_txn_not_visible_until_all_shards_contiguous(tmp_path):
    # s2 的订阅起始位置为 19（来自恢复检查点），其 LSN 20 迟到（19→21 先到）
    eng = OrderingEngine(gap_policy="wait", base_lsn={"s1": 10, "s2": 19})
    eng.ingest(ev("a1", "s1", 10, "insert", "T9", 1, "2026-11-01T00:02:00Z", pk="a"))
    eng.ingest(ev("a2", "s2", 21, "update", "T9", 2, "2026-11-01T00:02:00Z", pk="a"))
    eng.ingest(commit("c9", "s1", 11, "T9", "2026-11-01T00:02:00Z"))
    plan = eng.plan()
    assert plan["units"] == []                 # s2 有缺口，事务已提交但不可见
    assert plan["open_gaps"] == {"s2": [19, 20]}
    assert plan["stats"]["released_units"] == 0
    assert plan["stats"]["buffered"] >= 1      # 未确认事件占用缓存，可验证

    eng.ingest(ev("a0", "s2", 19, "insert", "T8", 1,
                  "2026-11-01T00:01:00Z", pk="z", autocommit=True))
    plan = eng.plan()
    # T8 的单语句事件在水位线之下，可先可见；但 T9 仍被 s2:20 缺口挡住
    assert [u["txn_id"] for u in plan["units"]] == ["T8"]
    assert plan["open_gaps"] == {"s2": [20]}
    eng.ingest(ev("a-mid", "s2", 20, "insert", "T9", 0,
                  "2026-11-01T00:02:00Z", pk="m"))
    plan = eng.plan()
    # T8 是单语句隐式提交事务；T9 跨分片等齐后按 commit_ts 全局排序
    assert [u["txn_id"] for u in plan["units"]] == ["T8", "T9"]


def test_uncommitted_txn_never_confirmed(tmp_path):
    eng = OrderingEngine()
    eng.ingest(ev("u1", "s1", 1, "insert", "TU", 1, pk="u"))
    eng.ingest(ev("u2", "s1", 2, "update", "TU", 2, pk="u"))
    plan = eng.plan()
    assert plan["units"] == []
    assert plan["stats"]["open_transactions"] == 1


def test_aborted_txn_events_dropped(tmp_path):
    eng = OrderingEngine()
    eng.ingest(ev("x1", "s1", 1, "insert", "TX", 1, pk="x"))
    eng.ingest(ev("abort-x", "s1", 2, "abort", "TX", 0, table="", pk=""))
    plan = eng.plan()
    assert plan["units"] == []
    assert len(plan["aborted"]) == 1
    assert plan["watermarks"]["s1"] == 3  # 中止事件也推进水位线，不卡死后续


# ---------------------------------------------------------------------------
# 3. 重复事件与同 LSN 冲突
# ---------------------------------------------------------------------------

def test_duplicate_event_ids_deduped():
    e = ev("dup", "s1", 1, "insert", "T1", 1, "2026-11-01T00:00:00Z", pk="d")
    plan = order_events([dict(e), dict(e), commit("c", "s1", 2, "T1",
                                                  "2026-11-01T00:00:00Z")])
    assert plan["stats"]["duplicates"] == 1
    assert len(plan["units"][0]["events"]) == 1


def test_same_lsn_different_identity_quarantined():
    # 同 LSN、同定序键（txn_seq/table/pk 全同）但投递身份不同：位置冲突，隔离
    plan = order_events([
        ev("e1", "s1", 1, "insert", "T1", 1, "2026-11-01T00:00:00Z", pk="a"),
        ev("e2", "s1", 1, "insert", "T1", 1, "2026-11-01T00:00:00Z", pk="a"),
    ])
    assert plan["quarantine"][0]["reason"] == "lsn_conflict"


def test_same_lsn_multi_row_dml_coexists_and_orders():
    # 同 LSN 不同主键是合法的多行 DML，按 order_key 共存定序，不视为冲突
    plan = order_events([
        ev("e2", "s1", 1, "insert", None, 1, "2026-11-01T00:00:00Z",
           pk="b", autocommit=True),
        ev("e1", "s1", 1, "insert", None, 1, "2026-11-01T00:00:00Z",
           pk="a", autocommit=True),
    ])
    assert plan["quarantine"] == []
    released = [e["pk"] for u in plan["units"] for e in u["events"]]
    assert released == ["a", "b"]


# ---------------------------------------------------------------------------
# 4. 缺口策略：等待 / 隔离 / 缓存预算
# ---------------------------------------------------------------------------

def test_wait_policy_holds_watermark_and_tracks_buffer():
    eng = OrderingEngine(gap_policy="wait", wait_budget=100, base_lsn={"s1": 1})
    eng.ingest(ev("w1", "s1", 1, "insert", "T1", 1, "2026-11-01T00:00:00Z", pk="w"))
    eng.ingest(ev("w3", "s1", 3, "insert", "T1", 2, "2026-11-01T00:00:00Z", pk="w"))
    plan = eng.plan()
    assert plan["watermarks"]["s1"] == 2       # 缺口在 2，水位线停住
    assert plan["stats"]["buffered"] >= 1      # 等待占用可验证
    assert plan["stats"]["buffer_peak"] >= 1
    assert plan["quarantine"] == []            # 预算内只等待不隔离


def test_wait_budget_exceeded_escalates_to_quarantine():
    eng = OrderingEngine(gap_policy="wait", wait_budget=1, base_lsn={"s1": 1})
    eng.ingest(ev("w1", "s1", 1, "insert", "T1", 1, "2026-11-01T00:00:00Z", pk="w"))
    eng.ingest(ev("w3", "s1", 3, "insert", "T1", 2, "2026-11-01T00:00:00Z", pk="w"))
    plan = eng.plan()
    reasons = {q["reason"] for q in plan["quarantine"]}
    assert "wait_budget_exceeded" in reasons


def test_quarantine_policy_fences_and_heals_on_late_arrival(tmp_path):
    eng = OrderingEngine(gap_policy="quarantine", base_lsn={"s1": 1})
    eng.ingest(ev("q1", "s1", 1, "insert", "T1", 1, "2026-11-01T00:00:00Z", pk="q"))
    eng.ingest(ev("q3", "s1", 3, "insert", "T2", 1, "2026-11-01T00:01:00Z", pk="q"))
    plan = eng.plan()
    assert any(q["reason"] == "gap_quarantine" for q in plan["quarantine"])
    assert plan["watermarks"]["s1"] == 2

    # 迟到补缺：隔离区事件自动重新接纳，事务随后可见
    eng.ingest(ev("q2", "s1", 2, "insert", "T1", 2, "2026-11-01T00:00:00Z", pk="q"))
    eng.ingest(commit("c1", "s1", 4, "T1", "2026-11-01T00:00:00Z"))
    eng.ingest(commit("c2", "s1", 5, "T2", "2026-11-01T00:01:00Z"))
    plan = eng.plan()
    assert not plan["open_gaps"]
    assert [u["txn_id"] for u in plan["units"]] == ["T1", "T2"]
    assert any(d.get("decision") == "heal" for d in plan["decisions"])


def test_implicit_anchor_shard_never_fences_before_first_event():
    # 没有显式订阅锚点时，首条事件之前的 LSN 空洞不属于本窗口，不得隔离
    eng = OrderingEngine(gap_policy="quarantine")
    eng.ingest(ev("late1", "s1", 50, "insert", "T1", 1,
                  "2026-11-01T00:00:00Z", pk="a"))
    eng.ingest(ev("late2", "s1", 52, "insert", "T2", 1,
                  "2026-11-01T00:01:00Z", pk="b"))
    plan = eng.plan()
    assert plan["quarantine"] == []


def test_quarantined_event_stays_fenced_while_gap_remains_open():
    # 其他分片的补缺事件不能把仍处缺口区间的隔离事件错误重新接纳
    eng = OrderingEngine(gap_policy="quarantine", base_lsn={"s1": 1, "s2": 1})
    eng.ingest(ev("g3", "s1", 3, "insert", "T1", 1,
                  "2026-11-01T00:00:00Z", pk="g"))
    assert [q["event"]["event_id"] for q in eng.plan()["quarantine"]] == ["g3"]
    eng.ingest(ev("other", "s2", 1, "insert", "T2", 1,
                  "2026-11-01T00:00:01Z", pk="o", autocommit=True))
    assert [q["event"]["event_id"] for q in eng.plan()["quarantine"]] == ["g3"]
    eng.ingest(ev("g1", "s1", 1, "insert", "T3", 1,
                  "2026-11-01T00:00:02Z", pk="g", autocommit=True))
    # 还差 s1:2，g3 仍在隔离区
    assert [q["event"]["event_id"] for q in eng.plan()["quarantine"]] == ["g3"]
    eng.ingest(ev("g2", "s1", 2, "insert", "T4", 1,
                  "2026-11-01T00:00:03Z", pk="g", autocommit=True))
    assert eng.plan()["quarantine"] == []  # 连续后自愈


def test_within_lsn_order_uses_txn_seq_table_pk():
    # 同 LSN 的多条 DML 按 txn_seq、table、pk 确定（题目要求的四级定序）
    events = [
        ev("b", "s1", 7, "update", "T1", 2, "2026-11-01T00:00:00Z",
           table="orders", pk="b"),
        ev("a", "s1", 7, "insert", "T1", 1, "2026-11-01T00:00:00Z",
           table="orders", pk="a"),
        commit("c", "s1", 8, "T1", "2026-11-01T00:00:00Z"),
    ]
    plan = order_events(events, base_lsn={"s1": 7})
    ids = [e["event_id"] for e in plan["units"][0]["events"]]
    assert ids == ["a", "b"]


def test_global_tie_break_by_txn_id_is_deterministic():
    events = [
        ev("x", "s1", 1, "insert", "TB", 1, "2026-11-01T00:00:00Z",
            pk="x", autocommit=True),
        ev("y", "s2", 1, "insert", "TA", 1, "2026-11-01T00:00:00Z",
            pk="y", autocommit=True),
    ]
    p1 = order_events(events)
    p2 = order_events(list(reversed(events)))
    assert [u["txn_id"] for u in p1["units"]] == \
           [u["txn_id"] for u in p2["units"]] == ["TA", "TB"]


def test_multistatement_txn_needs_explicit_commit_even_with_commit_ts():
    # 回归：多事件事务不能在首条事件后被隐式确认
    eng = OrderingEngine(base_lsn={"s1": 1})
    eng.ingest(ev("m1", "s1", 1, "insert", "TM", 1,
                  "2026-11-01T00:00:00Z", pk="m"))
    plan = eng.plan()
    assert plan["units"] == []
    eng.ingest(ev("m2", "s1", 2, "update", "TM", 2,
                  "2026-11-01T00:00:00Z", pk="m"))
    assert eng.plan()["units"] == []
    eng.ingest(commit("cm", "s1", 3, "TM", "2026-11-01T00:00:00Z"))
    assert [u["txn_id"] for u in eng.plan()["units"]] == ["TM"]


# ---------------------------------------------------------------------------
# 5. 全量切增量重叠窗口
# ---------------------------------------------------------------------------

def test_snapshot_overlap_suppresses_incremental():
    events = [
        # 全量快照行（s1:t:k），快照时间 00:05
        ev("snap", "s1", 5, "snapshot", None, 0, "2026-11-01T00:05:00Z",
           pk="k", is_snapshot=True, snapshot_ts="2026-11-01T00:05:00Z"),
        # 窗口内的迟到增量：commit_ts 早于快照，应被抑制
        ev("old", "s1", 4, "update", "Told", 1, "2026-11-01T00:04:00Z", pk="k"),
        commit("cold", "s1", 6, "Told", "2026-11-01T00:04:00Z"),
        # 快照之后的新变更：正常释放
        ev("new", "s1", 7, "update", "Tnew", 1, "2026-11-01T00:06:00Z", pk="k"),
        commit("cnew", "s1", 8, "Tnew", "2026-11-01T00:06:00Z"),
    ]
    # 全量切换：快照先建立窗口；增量流本身仍乱序（commit 控制事件先于 DML 到达）
    shuffled = [events[0], events[2], events[1], events[3], events[4]]
    plan = order_events(shuffled)
    suppressed_ids = {e["event_id"] for e in plan["suppressed"]}
    assert "old" in suppressed_ids
    released = {e["event_id"] for u in plan["units"] for e in u["events"]}
    assert "new" in released and "snap" in released and "old" not in released


# ---------------------------------------------------------------------------
# 6. 可重放：一次处理 == 多次恢复（指纹与最终镜像一致）
# ---------------------------------------------------------------------------

def _replay_scenario():
    return [
        ev("i1", "s1", 1, "insert", "T1", 1, "2026-11-01T00:01:00Z", pk="k1"),
        ev("i2", "s2", 1, "insert", "T2", 1, "2026-11-01T00:02:00Z", pk="k2"),
        ev("i1d", "s1", 2, "delete", "T3", 1, "2026-11-01T00:03:00Z", pk="k1"),
        commit("c1", "s1", 3, "T1", "2026-11-01T00:01:00Z"),
        commit("c2", "s2", 2, "T2", "2026-11-01T00:02:00Z"),
        commit("c3", "s1", 4, "T3", "2026-11-01T00:03:00Z"),
    ]


def test_deterministic_fingerprint():
    a = order_events(_replay_scenario())
    b = order_events(list(reversed(_replay_scenario())))
    assert a["fingerprint"] == b["fingerprint"]
    assert a["units"] == b["units"]


def test_chunked_recovery_matches_one_shot(tmp_path):
    events = _replay_scenario()

    con_one = make_con(tmp_path / "one")
    plan_one = order_events(events)
    checkpoint.commit_plan(con_one, plan_one)
    rows_one = mirror_rows(con_one)
    cps_one = store.load_checkpoints(con_one)
    con_one.close()

    # 分三批恢复，每批后持久化检查点并用其重建引擎
    con_rec = make_con(tmp_path / "rec")
    chunks = [events[0:2], events[2:4], events[4:]]
    state = None
    for chunk in chunks:
        eng = OrderingEngine.from_state(state) if state else OrderingEngine()
        eng.ingest_all(chunk)
        plan = eng.plan()
        checkpoint.commit_plan(con_rec, plan)
        eng.mark_durable([u["txn_id"] for u in plan["units"]])
        state = eng.state_dict()
    rows_rec = mirror_rows(con_rec)
    cps_rec = store.load_checkpoints(con_rec)
    con_rec.close()

    assert rows_one == rows_rec
    assert cps_one == cps_rec


def test_replay_same_plan_is_idempotent(tmp_path):
    con = make_con(tmp_path)
    plan = order_events(_replay_scenario())
    r1 = checkpoint.commit_plan(con, plan)
    r2 = checkpoint.commit_plan(con, plan)  # 重放同一计划
    assert r2["applied_events"] == 0
    assert r1["watermarks"] == r2["watermarks"]
    con.close()


# ---------------------------------------------------------------------------
# 7. 检查点原子性：失败回滚，检查点不得领先于数据
# ---------------------------------------------------------------------------

def test_commit_plan_atomic_rollback(tmp_path, monkeypatch):
    con = make_con(tmp_path)
    plan = order_events(_replay_scenario())
    assert plan["stats"]["released_events"] >= 2

    def boom(*_a, **_kw):
        raise RuntimeError("disk full")

    monkeypatch.setattr(store, "save_checkpoint", boom)
    with pytest.raises(RuntimeError):
        checkpoint.commit_plan(con, plan)

    # 回滚后：无镜像行、无已处理事务、无检查点
    assert store.mirror_count(con) == 0
    assert store.load_checkpoints(con) == {}
    con.close()


def test_checkpoint_monotonic_guard(tmp_path):
    con = make_con(tmp_path)
    store.save_checkpoint(con, "s1", 10)
    store.commit(con)
    plan = order_events([ev("z", "s1", 1, "insert", "T1", 1,
                            "2026-11-01T00:01:00Z", pk="z"),
                         commit("c", "s1", 2, "T1", "2026-11-01T00:01:00Z")])
    with pytest.raises(RuntimeError, match="regression"):
        checkpoint.commit_plan(con, plan)
    assert store.load_checkpoints(con) == {"s1": 10}
    con.close()


# ---------------------------------------------------------------------------
# 8. 既有 happy-path 行为保持不变
# ---------------------------------------------------------------------------

def test_order_process_legacy_contract():
    out = order.process({"id": "r-001"})
    assert out["_stage_order"] == "ok"
    assert order.process(out)["_stage_order"] == "ok"  # setdefault 不覆盖


def test_pipeline_happy_path_still_works():
    a = pipeline.run()
    b = pipeline.run()
    assert a["fed"] == 4
    assert b["rows"] == a["rows"] == 3
