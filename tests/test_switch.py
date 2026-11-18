"""全量↔增量安全切换协议测试。

覆盖：切换点两侧新增/更新/删除无漏无重、切换期间新事件、目标只确认半批退出、
重复发起切换、回滚边界与恢复后继续、增量回全量、旧输入（状态与记录）迁移、
每次阶段转移的依据（journal）。
"""
import pytest

from src.cdcmirror import store, switch
from src.cdcmirror.models import Record


@pytest.fixture()
def con(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "DB_PATH", str(tmp_path / "switch.db"))
    c = store.connect()
    try:
        yield c
    finally:
        c.close()


def ev(rid, pos, op="update", **data):
    return {"id": rid, "op": op, "pos": pos, **data}


def seed(con, rows):
    for key, data in rows.items():
        store.upsert(con, Record(key=key, data=data))
    con.commit()


def data_by_key(con):
    return dict(store.all_records(con))


def to_incremental(con, boundary=0):
    """空缓冲快速进入 INCREMENTAL 的辅助路径。"""
    switch.begin_switch(con, boundary)
    switch.begin_catchup(con)
    switch.complete_switch(con)


def test_full_to_incremental_no_gap_no_dup(con):
    seed(con, {"a": {"id": "a", "value": 1},
               "b": {"id": "b", "value": 2},
               "c": {"id": "c", "value": 3}})
    st = switch.begin_switch(con, 100, reason="全量完成，流位点 100")
    assert st["phase"] == "PREPARE" and st["switch_point"] == 100

    # 切换点两侧：≤100 已被快照覆盖 → 跳过；>100 → 暂停窗口缓冲
    assert switch.process(ev("a", 99, value=999), con=con)["_switch_action"] == "skip"
    held = [switch.process(e, con=con) for e in (
        ev("d", 101, op="insert", value=4),
        ev("a", 102, value=10),
        ev("c", 103, op="delete"),
        ev("b", 104, value=20),
    )]
    assert {r["_switch_action"] for r in held} == {"hold"}
    assert all(r["_hold"] for r in held)
    assert data_by_key(con)["a"]["value"] == 1  # 暂停窗口内目标不被事件改写

    switch.begin_catchup(con)
    switch.confirm_batch(con)
    assert switch.validate_target(con, expected_rows=3)["ok"]
    switch.complete_switch(con, expected_rows=3)

    data = data_by_key(con)
    assert data["a"]["value"] == 10 and data["b"]["value"] == 20
    assert data["d"]["value"] == 4 and "c" not in data  # 新增/更新/删除全部到位
    st = switch.status(con)
    assert st["phase"] == "INCREMENTAL" and st["confirmed_position"] == 104

    # 无重：重放 ≤ 已确认位点的事件不得覆盖已确认数据
    assert switch.process(ev("a", 102, value=999), con=con)["_switch_action"] == "skip"
    assert data_by_key(con)["a"]["value"] == 10
    # 无漏：切换后新事件直通过并推进确认位点
    assert switch.process(ev("b", 105, value=50), con=con)["_switch_action"] == "apply"
    assert data_by_key(con)["b"]["value"] == 50
    assert switch.status(con)["confirmed_position"] == 105

    journal = switch.transitions(con)
    path = [(j["from_phase"], j["to_phase"]) for j in journal]
    assert path == [("FULL", "PREPARE"), ("PREPARE", "CATCHUP"),
                    ("CATCHUP", "INCREMENTAL")]
    assert all(j["reason"] and j["at"] for j in journal)  # 每次转移有据可查


def test_events_arriving_during_catchup_must_be_drained(con):
    seed(con, {"a": {"id": "a", "value": 1}})
    switch.begin_switch(con, 100)
    switch.process(ev("a", 101, value=2), con=con)
    switch.begin_catchup(con)
    switch.confirm_batch(con)
    # 追平后、完成前又有新事件到达 → 未追平，拒绝完成
    switch.process(ev("b", 102, op="insert", value=9), con=con)
    with pytest.raises(switch.SwitchError):
        switch.complete_switch(con)
    switch.confirm_batch(con)  # 补确认后放行
    switch.complete_switch(con)
    assert data_by_key(con)["b"]["value"] == 9


def test_half_batch_exit_then_resume_no_reapply(con, monkeypatch):
    seed(con, {"a": {"id": "a", "value": 1}})
    switch.begin_switch(con, 100)
    for e in (ev("a", 101, value=10), ev("b", 102, op="insert", value=2),
              ev("c", 103, op="insert", value=3), ev("a", 104, value=40)):
        switch.process(e, con=con)
    switch.begin_catchup(con)
    r = switch.confirm_batch(con, limit=2)  # 目标只确认半批
    assert r["confirmed_position"] == 102
    con.close()  # 模拟退出（逐条提交，状态已持久化）

    con2 = store.connect()  # 恢复后继续
    st = switch.resume(con2)
    assert st["phase"] == "CATCHUP"
    assert st["confirmed_position"] == 102 and st["pending"] == 2
    assert st["next"] == "confirm"

    seen = []
    real_upsert = store.upsert

    def spy(c, record):
        seen.append(record.key)
        return real_upsert(c, record)

    monkeypatch.setattr(store, "upsert", spy)
    switch.confirm_batch(con2)
    assert seen == ["c", "a"]  # 只应用 103/104；已确认的 101/102 不重放
    switch.complete_switch(con2, expected_rows=3)
    data = data_by_key(con2)
    assert data["a"]["value"] == 40 and data["b"]["value"] == 2
    assert data["c"]["value"] == 3
    con2.close()


def test_duplicate_begin_switch_is_idempotent(con):
    switch.begin_switch(con, 100, reason="first")
    again = switch.begin_switch(con, 100, reason="dup")
    assert again["dedup"] is True and again["phase"] == "PREPARE"
    assert again["generation"] == 1
    assert len(switch.transitions(con)) == 1  # 重复发起不产生新的阶段转移
    with pytest.raises(switch.SwitchError):
        switch.begin_switch(con, 200)  # 边界冲突：须先回滚
    switch.begin_catchup(con)
    switch.complete_switch(con)
    with pytest.raises(switch.SwitchError):
        switch.begin_switch(con, 300)  # 已在增量阶段


def test_rollback_boundary_and_resume_point(con):
    seed(con, {"a": {"id": "a", "value": 1}})
    switch.begin_switch(con, 100)
    for e in (ev("a", 101, value=10), ev("b", 102, op="insert", value=2),
              ev("c", 103, op="insert", value=3), ev("a", 104, value=40)):
        switch.process(e, con=con)
    switch.begin_catchup(con)
    switch.confirm_batch(con, limit=2)  # 已确认到 102
    st = switch.rollback(con, reason="目标告警，回退")
    assert st["phase"] == "FULL" and st["confirmed_position"] == 102
    j = switch.transitions(con)[-1]
    assert (j["from_phase"], j["to_phase"]) == ("CATCHUP", "FULL")
    assert "102" in j["detail"]  # 回滚边界有据可查

    # 回退后从已确认水位继续：低于水位的边界被拒绝
    with pytest.raises(switch.SwitchError):
        switch.begin_switch(con, 50)
    switch.begin_switch(con, 102, reason="恢复后重试")
    # 源端重发：≤102 跳过（不重放已确认），>102 重新入缓冲
    assert switch.process(ev("a", 101, value=999), con=con)["_switch_action"] == "skip"
    switch.process(ev("c", 103, op="insert", value=3), con=con)
    switch.process(ev("a", 104, value=40), con=con)
    switch.begin_catchup(con)
    switch.confirm_batch(con)
    switch.complete_switch(con, expected_rows=3)
    data = data_by_key(con)
    assert data["a"]["value"] == 40 and data["c"]["value"] == 3


def test_incremental_back_to_full_and_recatchup(con, monkeypatch):
    to_incremental(con, boundary=0)
    switch.process(ev("a", 1, op="insert", value=1), con=con)
    switch.process(ev("a", 2, value=2), con=con)
    assert data_by_key(con)["a"]["value"] == 2

    switch.begin_reload(con, reason="schema 漂移，回全量")  # 暂停窗口开启
    again = switch.begin_reload(con)
    assert again["dedup"] is True and again["phase"] == "PAUSE"  # 重复发起幂等
    switch.process(ev("a", 3, value=3), con=con)  # 暂停期间事件 → 缓冲
    assert data_by_key(con)["a"]["value"] == 2  # 暂停窗口内不改写目标
    switch.begin_snapshot(con)  # RELOAD：全量重载
    switch.process(ev("b", 4, op="insert", value=4), con=con)
    assert "b" not in data_by_key(con)
    # 快照类记录在 RELOAD 阶段放行（由编排层落库）
    r = switch.process({"id": "s-1", "name": "snap"}, con=con)
    assert r["_switch_action"] == "apply" and "_hold" not in r
    # 全量重载落库（源端位点 4 的快照，已含事件 3、4 的效果）
    seed(con, {"a": {"id": "a", "value": 3}, "b": {"id": "b", "value": 4}})

    switch.begin_recatchup(con, 4)  # 新边界 4：事件 3、4 被快照覆盖
    switch.process(ev("a", 5, value=5), con=con)

    seen = []
    real_upsert = store.upsert

    def spy(c, record):
        seen.append(record.data.get("pos"))
        return real_upsert(c, record)

    monkeypatch.setattr(store, "upsert", spy)
    switch.confirm_batch(con)
    assert seen == [5]  # 事件 3、4 不重放（已被新快照覆盖）
    switch.complete_reload(con, expected_rows=2)
    data = data_by_key(con)
    assert data["a"]["value"] == 5 and data["b"]["value"] == 4

    path = [(j["from_phase"], j["to_phase"]) for j in switch.transitions(con)]
    assert ("INCREMENTAL", "PAUSE") in path and ("PAUSE", "RELOAD") in path
    assert ("RELOAD", "RECATCHUP") in path and ("RECATCHUP", "INCREMENTAL") in path


def test_reload_rollback_returns_to_pause_then_incremental(con):
    to_incremental(con, boundary=0)
    switch.process(ev("a", 1, op="insert", value=1), con=con)
    switch.process(ev("a", 2, value=2), con=con)
    switch.begin_reload(con)
    switch.process(ev("a", 3, value=3), con=con)  # 缓冲
    switch.begin_snapshot(con)
    st = switch.rollback(con, reason="重载中止")  # RELOAD → PAUSE
    assert st["phase"] == "PAUSE"
    st = switch.rollback(con, reason="放弃回全量")  # PAUSE → INCREMENTAL
    assert st["phase"] == "INCREMENTAL" and st["confirmed_position"] == 2
    # 未确认缓冲已丢弃；源端从已确认水位 +1 重发
    assert switch.process(ev("a", 3, value=3), con=con)["_switch_action"] == "apply"
    assert data_by_key(con)["a"]["value"] == 3


def test_legacy_state_migration(tmp_path, monkeypatch):
    # 旧版状态表：switch_state(id, phase, boundary)，阶段名 bulk/tail
    monkeypatch.setattr(store, "DB_PATH", str(tmp_path / "legacy_tail.db"))
    con = store.connect()
    con.execute("CREATE TABLE switch_state(id INTEGER PRIMARY KEY CHECK(id=1),"
                " phase TEXT NOT NULL, boundary INTEGER NOT NULL)")
    con.execute("INSERT INTO switch_state(id, phase, boundary) VALUES(1, 'tail', 40)")
    con.commit()
    st = switch.status(con)
    assert st["phase"] == "INCREMENTAL"
    assert st["source_boundary"] == 40 and st["confirmed_position"] == 40
    assert st["schema_version"] == 2
    assert any("migrate" in j["reason"] for j in switch.transitions(con))
    assert con.execute("SELECT COUNT(*) FROM switch_state_v1").fetchone()[0] == 1
    con.close()

    monkeypatch.setattr(store, "DB_PATH", str(tmp_path / "legacy_bulk.db"))
    con = store.connect()
    con.execute("CREATE TABLE switch_state(id INTEGER PRIMARY KEY CHECK(id=1),"
                " phase TEXT NOT NULL, boundary INTEGER NOT NULL)")
    con.execute("INSERT INTO switch_state(id, phase, boundary) VALUES(1, 'bulk', 7)")
    con.commit()
    st = switch.status(con)
    assert st["phase"] == "FULL" and st["confirmed_position"] == 7
    con.close()


def test_legacy_record_migration(con):
    legacy = {"id": "r-1", "src": "Alpha", "name": " Old ", "value": 7}
    r = switch.process(dict(legacy), con=con)  # FULL 阶段：旧输入升级为快照类
    assert r["_migrated"] is True and r["op"] == "snapshot"
    assert r["_switch_action"] == "apply" and "_hold" not in r
    to_incremental(con, boundary=10)
    r2 = switch.process(dict(legacy), con=con)  # 增量阶段：陈旧快照不得落库
    assert r2["_switch_action"] == "skip" and r2["_hold"] is True
    assert store.count(con) == 0  # 未覆盖任何已确认数据


def test_process_reuses_reader_and_checkpoint_contracts(con):
    r = switch.process({"id": "x", "op": "insert", "pos": 1}, con=con)
    assert r["_stage_switch"] == "ok"
    assert r["_stage_cdc_reader"] == "naive"  # 复用读取契约
    assert r["_stage_checkpoint"] == "naive"  # 复用检查点契约
    assert r["_switch_action"] == "hold"  # FULL 阶段事件入暂停窗口
    assert switch.status(con)["pending"] == 1


def test_journal_records_every_transition_with_reason(con):
    switch.begin_switch(con, 10, reason="快照完成")
    switch.rollback(con, reason="演练回滚")
    journal = switch.transitions(con)
    assert [(j["from_phase"], j["to_phase"]) for j in journal] == [
        ("FULL", "PREPARE"), ("PREPARE", "FULL")]
    assert all(j["reason"] and j["at"] for j in journal)
    assert journal[-1]["confirmed_position"] == 0
