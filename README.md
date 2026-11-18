# cdcmirror — 源库 binlog/CDC 增量镜像到目标，schema 漂移处理，全量↔增量切换与校验回滚。

运行栈：Python 3.14 + 标准库 `sqlite3` + `httpx` + `pytest`（离线，无需联网）

## 运行方式
读 `fixtures/input.json` → `loader` → `normalizer` → 其余已实现变换 → `keyer` → 幂等落 `sqlite`。
```text
make run     # 运行离线 happy-path，输出摄取数量和落库行数
make test    # 运行项目测试，包括流水线、幂等和边界桩
```

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段，不在编排层加入业务判断。
业务模块：`schema_drift`, `switch`, `cdc_reader`, `checkpoint`, `verify`, `order`。
通用模块：`loader / normalizer / keyer / store / pipeline / cli`。

## 当前实现状态
- `schema_drift.py`：`process()` 当前抛出 `NotImplementedError`，未接入基础 happy-path。
- `switch.py`：`process()` 当前抛出 `NotImplementedError`，未接入基础 happy-path。
- `cdc_reader.py`：提供基础 `process()`，复制输入记录并补充 `_stage_cdc_reader` 阶段标记。
- `checkpoint.py`：记录级 `process()` 保持基线行为；新增 `commit_plan(con, plan)`，
  在同一个 sqlite 事务内原子完成镜像写入、事务处理标记与分片水位线检查点，失败整体回滚。
- `verify.py`：提供基础 `process()`，复制输入记录并补充 `_stage_verify` 阶段标记。
- `order.py`：记录级 `process()` 保持基线行为；新增批式 `OrderingEngine` / `order_events`。

### order 阶段：binlog/CDC 事件排序与事务整体可见性
- **分片局部顺序**：同分片按 `(lsn, txn_seq, table, pk)` 定序，LSN 缺口处水位线停住。
- **事务整体可见性**：事务（可跨分片）仅在显式提交/`autocommit`/快照，且每个参与分片
  的事件 LSN 都低于连续水位线时，才按 `(commit_ts, txn_id)` 全局顺序进入计划；
  未提交、已中止事务绝不提前确认。
- **缺口策略（可验证）**：`wait` 缓存等待并用 `buffer_peak`/`wait_budget` 给出可核验的
  缓存占用与升级边界；`quarantine` 立即隔离缺口边缘事件，迟到补缺后按连续性自愈（heal），
  全部决策在 `plan()["decisions"]` 与 `explain()` 中留痕。
- **重复与冲突**：`event_id` 去重；同 `(shard, lsn)` 同业务定序键的不同身份隔离，
  同 LSN 不同主键的多行 DML 合法共存。
- **全量切增量重叠窗口**：快照已覆盖的增量变更按 `(shard, table, pk)` 抑制并记录。
- **可重放**：计划是输入集合与配置的纯函数，附 SHA-256 `fingerprint`；
  `state_dict()`/`from_state()` 支持崩溃恢复，一次处理与分多次恢复落库结果一致
  （见 `tests/test_order.py`）。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
