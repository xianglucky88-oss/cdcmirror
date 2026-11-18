"""verify.py 一致性校验引擎测试。

覆盖：同一比较边界、行数相同但内容不符、删除事件丢失 vs 未消费尾部、
分片超时隔离、断点续检、修复建议、失败不污染已确认批次、
重启/回滚/接管后已确认数据不变、旧入口 process() 照常可用。
"""
import json
import sqlite3

from src.cdcmirror import verify
from src.cdcmirror.verify import (
    InMemoryTarget,
    ReadBatch,
    ShardTimeoutError,
    SqliteTarget,
    Verifier,
    bucket_digest,
    comparison_boundary,
    row_digest,
)

FIX = "fixtures/verify"


def load(path):
    return verify.load_case(path)


# ---------------------------------------------------------------- 旧入口
def test_legacy_process_entry_unchanged():
    rec = verify.process({"id": "x"})
    assert rec["_stage_verify"] == "ok"
    assert rec["id"] == "x"
    # 已有标记不被覆盖（setdefault 语义）
    assert verify.process({"_stage_verify": "custom"})["_stage_verify"] == "custom"


# ---------------------------------------------------------------- 比较边界
def test_boundary_is_min_of_reader_highwater_and_checkpoint():
    batches = [ReadBatch.from_dict({
        "batch_id": "b1", "shard": "s1",
        "events": [{"seq": 7, "tx": "t", "table": "a", "pk": "p", "op": "insert",
                    "payload": {}},
                   {"seq": 9, "tx": "t", "table": "a", "pk": "q", "op": "insert",
                    "payload": {}}],
    })]
    # reader 高水位 9，检查点 5 → 边界 5
    assert comparison_boundary(batches, {"s1": 5}) == {"s1": 5}
    # 检查点领先 reader → 边界回落到 reader 高水位
    assert comparison_boundary(batches, {"s1": 99}) == {"s1": 9}
    # 无检查点 → 边界 0（全部视为未消费尾部）
    assert comparison_boundary(batches, {}) == {"s1": 0}


def test_report_derives_one_boundary_from_batches_checkpoints_and_target():
    _, batches, checkpoints, target = load(f"{FIX}/missing_delete.json")
    v = Verifier()
    rep = v.run(run_id="r", read_batches=batches, checkpoints=checkpoints, target=target)
    # reader_hw=5, checkpoint=4 → 同一边界 4；目标已应用位点 4 不越界
    assert rep.boundary == {"s1": 4}
    assert rep.shards["s1"].target_high_water == 4
    assert rep.shards["s1"].warnings == []


# ---------------------------------------------------------------- 行数相同 ≠ 镜像一致
def test_same_row_count_but_pk_and_content_differ():
    _, batches, checkpoints, target = load(f"{FIX}/count_same_content_diff.json")
    v = Verifier()
    rep = v.run(run_id="r", read_batches=batches, checkpoints=checkpoints, target=target)

    assert rep.status == "mismatch"
    shard = rep.shards["s1"]
    tbl = shard.tables["acct"]
    # 行数相同(4=4)，但摘要与主键范围不一致 → 行数不能说明一致
    assert tbl["count_source"] == tbl["count_target"] == 4
    assert tbl["digest_match"] is False
    assert tbl["pk_range_source"] == ["a1", "a4"]
    assert tbl["pk_range_target"] == ["a1", "a9"]

    # 下钻定位到具体主键
    kinds = {(d.kind, d.pk) for d in shard.discrepancies}
    assert ("mismatch", "a2") in kinds          # 内容不符（漏了 seq4 更新）
    assert ("lost", "a4") in kinds              # 真正丢失（漏了 seq5 插入）
    assert ("unexpected_row", "a9") in kinds    # 目标多出演化外的行
    a2 = next(d for d in shard.discrepancies if d.pk == "a2")
    assert a2.missed_events == [4]
    assert a2.expected == {"bal": 250} and a2.actual == {"bal": 200}

    # 修复建议：覆盖 a2、重放 a4、排查 a9
    actions = {(r["action"], r.get("pk")) for r in shard.repair}
    assert ("upsert_row", "a2") in actions
    assert ("replay_events", "a4") in actions
    assert ("investigate_row", "a9") in actions


def test_bucket_digest_drills_down_to_row_level():
    # 桶摘要一致 ⇒ 行集合与内容一致；不一致 ⇒ 行级摘要能指出具体主键
    d1 = {"a": row_digest({"v": 1}), "b": row_digest({"v": 2})}
    d2 = {"a": row_digest({"v": 1}), "b": row_digest({"v": 3})}
    assert bucket_digest(d1) != bucket_digest(d2)
    assert bucket_digest(d1) == bucket_digest(dict(d1))
    changed = [pk for pk in d1 if d1[pk] != d2.get(pk)]
    assert changed == ["b"]


# ---------------------------------------------------------------- 删除丢失 vs 未消费尾部
def test_missing_delete_distinguished_from_pending_tail():
    _, batches, checkpoints, target = load(f"{FIX}/missing_delete.json")
    v = Verifier()
    rep = v.run(run_id="r", read_batches=batches, checkpoints=checkpoints, target=target)

    assert rep.status == "mismatch"
    shard = rep.shards["s1"]
    # u1 的 delete(seq3) 在边界内未生效 → lost_delete，建议补删
    (disc,) = shard.discrepancies
    assert disc.kind == "lost_delete" and disc.pk == "u1" and disc.seq == 3
    assert {"action": "delete_row", "table": "users", "pk": "u1",
            "reason": "补应用丢失的删除事件"} in shard.repair
    # seq5(u4) 越过边界 → pending（尚未追平），不算丢失
    assert shard.pending_events == [5]
    assert all(d.pk != "u4" for d in shard.discrepancies)
    assert any(r["action"] == "wait_recheck" and r["events"] == [5]
               for r in shard.repair)


# ---------------------------------------------------------------- 分片超时：部分失败隔离 + 断点续检
class CountingTarget:
    """包装目标存储，记录每个分片被读取的次数（验证续检不重检 ok 分片）。"""

    def __init__(self, inner):
        self.inner = inner
        self.calls = {}

    def rows_for(self, shard):
        self.calls[shard] = self.calls.get(shard, 0) + 1
        return self.inner.rows_for(shard)

    def high_water(self, shard):
        return self.inner.high_water(shard)


def test_shard_timeout_isolated_then_resume_rechecks_only_failed_shard(tmp_path):
    meta, batches, checkpoints, target = load(f"{FIX}/shard_timeout.json")
    db = str(tmp_path / "verify.db")

    # 第一次运行：s2 超时 → 部分失败隔离
    v1 = Verifier(sqlite3.connect(db))
    rep1 = v1.run(run_id="run-1", read_batches=batches,
                  checkpoints=checkpoints, target=target)
    assert rep1.status == "incomplete"
    assert rep1.shards["s1"].status == "ok"
    assert rep1.shards["s2"].status == "uncheckable"
    assert isinstance(rep1.shards["s2"].error, str)
    assert any(r["action"] == "retry_shard" for r in rep1.shards["s2"].repair)
    # 隔离语义：健康分片的批次被确认，故障分片的批次不确认
    assert "b1" in rep1.confirmed_batches
    assert "b3" not in rep1.confirmed_batches
    v1.close()

    # s2 恢复后断点续检：s1 复用不重读，s2 补检
    healed = InMemoryTarget(meta["target_rows"])      # 不再超时
    counting = CountingTarget(healed)
    v2 = Verifier(sqlite3.connect(db))                # 重启后接管同一状态库
    rep2 = v2.run(run_id="run-1", read_batches=batches,
                  checkpoints=checkpoints, target=counting)
    assert rep2.status == "ok"
    assert rep2.reused_shards == ["s1"]
    assert rep2.shards["s1"].reused is True
    assert counting.calls.get("s1", 0) == 0           # ok 分片未被重读
    assert counting.calls.get("s2", 0) == 1           # 仅补检失败分片
    assert set(rep2.confirmed_batches) == {"b1", "b3"}
    v2.close()


def test_shard_unexpected_exception_is_also_isolated():
    class BrokenTarget:
        def rows_for(self, shard):
            raise RuntimeError("disk gone")

        def high_water(self, shard):
            raise RuntimeError("disk gone")

    batches = [ReadBatch.from_dict({
        "batch_id": "b1", "shard": "s1",
        "events": [{"seq": 1, "tx": "t", "table": "a", "pk": "p", "op": "insert",
                    "payload": {}}],
    })]
    v = Verifier()
    rep = v.run(run_id="r", read_batches=batches, checkpoints={"s1": 1},
                target=BrokenTarget())
    assert rep.status == "incomplete"
    assert rep.shards["s1"].status == "uncheckable"
    assert "disk gone" in rep.shards["s1"].error


# ---------------------------------------------------------------- 失败不污染已确认批次
def test_failed_run_does_not_pollute_confirmed_batches(tmp_path):
    meta, batches, checkpoints, _ = load(f"{FIX}/shard_timeout.json")
    db = str(tmp_path / "verify.db")
    healthy = InMemoryTarget(meta["target_rows"])

    # 运行一：全部一致 → 两个批次都确认
    v = Verifier(sqlite3.connect(db))
    ok = v.run(run_id="good", read_batches=batches,
               checkpoints=checkpoints, target=healthy)
    assert ok.status == "ok"
    assert set(ok.confirmed_batches) == {"b1", "b3"}

    # 运行二：s2 超时且换一个 run_id → 既有确认记录原样保留
    degraded = InMemoryTarget(meta["target_rows"], unavailable_shards=["s2"])
    bad = v.run(run_id="bad", read_batches=batches,
                checkpoints=checkpoints, target=degraded)
    assert bad.status == "incomplete"
    assert set(bad.confirmed_batches) == {"b1", "b3"}   # 未被污染/回滚
    assert v.confirmed_batches() == ["b1", "b3"]
    v.close()


def test_verification_never_writes_to_target(tmp_path):
    # 校验全程只读：目标存储内容在校验前后逐字节一致
    con = sqlite3.connect(str(tmp_path / "target.db"))
    target = SqliteTarget(con)
    rows = [{"table": "acct", "pk": "a1", "payload": {"bal": 1}, "source_seq": 1}]
    target.replace_rows("s1", rows)
    before = con.execute(
        "SELECT shard,tbl,pk,payload,source_seq FROM target_rows ORDER BY 1,2,3"
    ).fetchall()

    batches = [ReadBatch.from_dict({
        "batch_id": "b1", "shard": "s1",
        "events": [{"seq": 1, "tx": "t", "table": "acct", "pk": "a1",
                    "op": "insert", "payload": {"bal": 1}}],
    })]
    v = Verifier()
    rep = v.run(run_id="r", read_batches=batches, checkpoints={"s1": 1}, target=target)
    assert rep.status == "ok"
    after = con.execute(
        "SELECT shard,tbl,pk,payload,source_seq FROM target_rows ORDER BY 1,2,3"
    ).fetchall()
    assert before == after
    con.close()


# ---------------------------------------------------------------- 重启 / 回滚 / 接管
def test_restart_rollback_takeover_keeps_confirmed_data(tmp_path):
    state_db = str(tmp_path / "verify_state.db")
    target_db = str(tmp_path / "target.db")

    full_rows = [
        {"table": "acct", "pk": "a1", "payload": {"bal": 1}, "source_seq": 1},
        {"table": "acct", "pk": "a2", "payload": {"bal": 2}, "source_seq": 2},
    ]
    rolled_back_rows = full_rows[:1]      # 回滚到只含 a1 的旧快照

    batches = [ReadBatch.from_dict({
        "batch_id": "b1", "shard": "s1",
        "events": [
            {"seq": 1, "tx": "t1", "table": "acct", "pk": "a1",
             "op": "insert", "payload": {"bal": 1}},
            {"seq": 2, "tx": "t1", "table": "acct", "pk": "a2",
             "op": "insert", "payload": {"bal": 2}},
        ],
    })]
    checkpoints = {"s1": 2}

    # 阶段一：一致 → b1 确认
    tcon = sqlite3.connect(target_db)
    target = SqliteTarget(tcon)
    target.replace_rows("s1", full_rows)
    v1 = Verifier(sqlite3.connect(state_db))
    rep1 = v1.run(run_id="run-x", read_batches=batches,
                  checkpoints=checkpoints, target=target)
    assert rep1.status == "ok" and rep1.confirmed_batches == ["b1"]
    v1.close()
    tcon.close()                          # ← 模拟重启

    # 阶段二：目标回滚到旧快照，新校验器接管（新连接、新实例）
    tcon2 = sqlite3.connect(target_db)
    target2 = SqliteTarget(tcon2)
    target2.replace_rows("s1", rolled_back_rows)
    v2 = Verifier(sqlite3.connect(state_db))
    rep2 = v2.run(run_id="run-x", read_batches=batches,
                  checkpoints=checkpoints, target=target2, resume=False)

    # 回滚被检出：a2 在边界内丢失；且已确认批次记录保持不变
    assert rep2.status == "mismatch"
    (disc,) = rep2.shards["s1"].discrepancies
    assert disc.kind == "lost" and disc.pk == "a2"
    assert v2.confirmed_batches() == ["b1"]          # 已确认数据保持
    assert rep2.confirmed_batches == ["b1"]

    # 校验本身未改动回滚后的目标内容
    assert target2.rows_for("s1") == [
        {"table": "acct", "pk": "a1", "payload": {"bal": 1}, "source_seq": 1}
    ]
    v2.close()
    tcon2.close()


# ---------------------------------------------------------------- 跨边界批次不确认
def test_batch_straddling_boundary_is_not_confirmed():
    batches = [ReadBatch.from_dict({
        "batch_id": "b-tail", "shard": "s1",
        "events": [
            {"seq": 1, "tx": "t", "table": "a", "pk": "p", "op": "insert",
             "payload": {"v": 1}},
            {"seq": 2, "tx": "t", "table": "a", "pk": "q", "op": "insert",
             "payload": {"v": 2}},     # 越过检查点 → 未消费尾部
        ],
    })]
    target = InMemoryTarget([
        {"shard": "s1", "table": "a", "pk": "p", "payload": {"v": 1},
         "source_seq": 1},
    ])
    v = Verifier()
    rep = v.run(run_id="r", read_batches=batches, checkpoints={"s1": 1},
                target=target)
    assert rep.status == "ok"                      # 尾部 pending 不算失配
    assert rep.shards["s1"].pending_events == [2]
    assert rep.confirmed_batches == []             # 跨边界批次不确认


# ---------------------------------------------------------------- 报告可序列化（对账报告）
def test_report_is_json_serializable():
    _, batches, checkpoints, target = load(f"{FIX}/count_same_content_diff.json")
    rep = Verifier().run(run_id="r", read_batches=batches,
                         checkpoints=checkpoints, target=target)
    text = json.dumps(rep.to_dict(), ensure_ascii=False)
    assert json.loads(text)["status"] == "mismatch"
