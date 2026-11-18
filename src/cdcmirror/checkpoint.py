"""单一职责：检查点与原子提交边界。

记录级 ``process`` 保持基线朴素行为（happy-path 不变）。

批式 ``commit_plan`` 消费 order 阶段输出的有序计划，在**同一个 sqlite 事务**内
完成：镜像行写入 → 事务处理标记 → 分片水位线检查点 → 计划指纹核验。任何一步
失败都整体回滚——检查点不会领先于数据，数据也不会在无检查点的情况下被确认。
因此一次处理与崩溃后的多次恢复产生相同的落库结果（可重放）。
"""
from . import store


def process(record):
    rec = dict(record)
    rec.setdefault("_stage_checkpoint", "naive")
    return rec


def commit_plan(con, plan) -> dict:
    """原子地应用有序计划并推进检查点。

    返回应用统计；失败时抛出异常并保证连接回滚到调用前状态。
    """
    store.ensure_cdc_tables(con)
    try:
        before = store.load_checkpoints(con)
        applied_units, applied_events, skipped_events = 0, 0, 0

        # 1) 按计划中的全局提交顺序逐事务落库（顺序由 order 阶段决定）
        for unit in plan["units"]:
            res = store.apply_unit(con, unit)
            applied_events += res["applied"]
            skipped_events += res["skipped"]
            if res["applied"]:
                applied_units += 1
            store.mark_txn(con, unit["txn_id"], unit["commit_ts"], plan["fingerprint"])

        # 2) 检查点只允许单调前进：落后/相同水位线不得回退
        for shard, watermark in plan["watermarks"].items():
            if shard in before and before[shard] > watermark:
                raise RuntimeError(
                    f"checkpoint regression on shard {shard!r}: "
                    f"{watermark} < {before[shard]}"
                )
            store.save_checkpoint(con, shard, watermark)

        # 3) 数据与检查点同事务提交
        store.commit(con)
    except Exception:
        con.rollback()
        raise

    return {
        "applied_units": applied_units,
        "applied_events": applied_events,
        "skipped_events": skipped_events,
        "watermarks": store.load_checkpoints(con),
        "fingerprint": plan["fingerprint"],
        "quarantined": len(plan.get("quarantine", ())),
        "duplicates": len(plan.get("duplicates", ())),
        "suppressed": len(plan.get("suppressed", ())),
    }
