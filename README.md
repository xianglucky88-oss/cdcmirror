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
- `checkpoint.py`：提供基础 `process()`，复制输入记录并补充 `_stage_checkpoint` 阶段标记。
- `verify.py`：`process()` 阶段标记入口保持不变；另内置源端↔目标端一致性校验引擎（见下节）。
- `order.py`：提供基础 `process()`，复制输入记录并补充 `_stage_order` 阶段标记。

当前未实现阶段未进入基础流水线，因此 `make run` 仍可运行已接入的阶段。

## 一致性校验（verify.py）
```text
make verify   # 对 fixtures/verify/ 三个样例出对账报告；检出差异时退出码为 1（样例故意含差异）
```
- **同一比较边界**：`boundary = min(reader 高水位, checkpoint 位点)`，由读取批次、检查点、
  目标存储三方取得。边界内必须账实相符；边界外是「尚未追平」(pending)，不是丢失。
- **分层摘要保定位**：表级先比 行数/主键范围/桶摘要（省成本）；桶摘要失配时下钻行级摘要，
  定位到具体主键与责任事件 seq——行数相同不代表一致（见样例一，4=4 但内容/主键不同）。
- **差异分类**：`pending`（未消费尾部）/ `lost`（真正丢失）/ `mismatch`（内容不符）/
  `lost_delete`（删除丢失）/ `unexpected_row`（目标多行）；分片异常/超时记 `uncheckable`。
- **分片校验与部分失败隔离**：单分片故障只标记本分片，其余分片照常出结果并确认批次。
- **断点续检**：分片结果落 sqlite，同 `run_id` 重跑只补检未 ok 分片；已确认批次只增不改，
  失败运行不污染，校验全程只读目标存储。重启/回滚/接管后已确认数据保持不变。
- **修复建议**（只建议不执行）：`replay_events` / `upsert_row` / `delete_row` /
  `investigate_row` / `wait_recheck` / `retry_shard`。

样例：`fixtures/verify/count_same_content_diff.json`（行数相同但主键/内容不同）、
`missing_delete.json`（删除事件丢失 + 未消费尾部）、`shard_timeout.json`（部分分片超时）。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
