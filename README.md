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
- `cdc_reader.py`：增强为长连接 CDC 读取器（`CDCReader`），同时保留基线 `process()` 阶段函数。
- `checkpoint.py`：提供基础 `process()`，复制输入记录并补充 `_stage_checkpoint` 阶段标记。
- `verify.py`：提供基础 `process()`，复制输入记录并补充 `_stage_verify` 阶段标记。
- `order.py`：提供基础 `process()`，复制输入记录并补充 `_stage_order` 阶段标记。

### CDC 读取器语义（`cdc_reader.py`）
- **位点只在下游确认后推进**：`sink.deliver()` → `sink.commit()` 成功后才写 checkpoint；
  心跳与空批不出数、不推进位点；事务提交位点推进到 `COMMIT` 边界。
- **断线重连与事务边界**：`TransientDisconnect` 可重试，重连从最后确认位点继续，
  未确认的半个事务丢弃缓冲、整体重放；`BEGIN/DATA/COMMIT` 顺序受校验，嵌套事务、
  事务外 data、无 begin 的 commit 一律按坏事件处理。
- **诊断分级**：`CheckpointUnavailable`（位点失效）与 `BadEvent`（坏事件）不可重试、
  读到即停；`TransientDisconnect` 可重试，超过 `max_reconnects` 后抛出，调用方据此决定停读还是重试。
- **大事务资源边界**：`max_buffer_events` / `max_buffer_bytes` 限定缓冲，超限按子批
  「先确认后位点」拆分，中段位点带 `open_txn` 标记；`buffer_stats()` 暴露当前占用与水位线；
  单事件超字节上限直接 `BufferLimitExceeded`。
- **并发/重入**：`pump()` 串行化，重入调用返回下游已确认序列的快照（`reentrant=True`），
  所有调用方看到同一事件序列，不会重复投递。
- 位点存储提供 `MemoryCheckpoint`（含推进审计，可用目标提交记录反查每次位点推进）
  与 `SqliteCheckpoint`；下游参考实现 `IdempotentMemorySink` 验证 at-least-once 重放可安全吸收。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
CDC 故障注入测试位于 `tests/test_cdc_reader.py`，全程使用脚本化离线连接替身
（心跳 → 半个事务 → 断线 → 重连重放、位点写失败、坏事件、位点失效、空批、大事务拆分、并发重入），无需联网。
