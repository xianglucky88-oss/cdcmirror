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
- `switch.py`：全量↔增量安全切换协议（已实现并接入流水线，见下节）。
- `cdc_reader.py`：提供基础 `process()`，复制输入记录并补充 `_stage_cdc_reader` 阶段标记。
- `checkpoint.py`：提供基础 `process()`，复制输入记录并补充 `_stage_checkpoint` 阶段标记。
- `verify.py`：提供基础 `process()`，复制输入记录并补充 `_stage_verify` 阶段标记。
- `order.py`：提供基础 `process()`，复制输入记录并补充 `_stage_order` 阶段标记。

当前未实现阶段（schema_drift）未进入基础流水线，因此 `make run` 仍可运行已接入的阶段。

## 全量↔增量切换协议（switch.py）

阶段机：
- 全量切增量：`FULL → PREPARE → CATCHUP → INCREMENTAL`
- 增量回全量：`INCREMENTAL → PAUSE → RELOAD → RECATCHUP → INCREMENTAL`
- 回滚：`PREPARE/CATCHUP → FULL`，`PAUSE → INCREMENTAL`，`RELOAD/RECATCHUP → PAUSE`

切换点：`begin_switch(boundary)` 记录源边界与切换点；位点 ≤ boundary 的数据由快照
覆盖一律跳过，位点 > boundary 的事件进入暂停窗口（`switch_buffer` 缓冲，不直接应用）。

双阶段校验：
1. 源侧（`begin_catchup` / `validate_source`）：未确认缓冲位点必须从 boundary+1
   连续（无空洞），否则拒绝进入追平。
2. 目标侧（`complete_switch` / `complete_reload` / `validate_target`）：必须满足
   追平条件。

追平条件：缓冲区无未确认事件 且 已确认水位 ≥ catchup_goal（进入追平时捕获的流头）。
已确认水位 = max(confirmed_position, source_boundary)。追平期间新到事件一并入缓冲，
须一并确认后才算追平。

回滚边界与回退后继续点：回滚边界 = 已确认水位。回滚丢弃未确认缓冲（已确认数据
不受影响）；恢复后从 已确认水位 + 1 继续——位点 ≤ 水位 的事件一律跳过（不重复
覆盖已确认数据），源端只需从 已确认水位 + 1 重发，再次 `begin_switch` 的 boundary
不得低于已确认水位。

持久化（与业务库同一 sqlite）：`switch_state`（单行：阶段/源边界/切换点/目标确认
位点）、`switch_journal`（每次阶段转移的依据）、`switch_buffer`（暂停窗口内事件，
按位点去重）。追平确认逐条提交：目标只确认半批时退出，恢复后 `resume()` 提示下一
步，从未确认位点继续，已确认部分绝不重放。重复发起切换幂等（同边界去重，冲突边界
报错须先回滚）。旧版 `switch_state(id, phase, boundary)` 状态与无 op/pos 信封的旧
输入记录会在读取时自动迁移。

不变量：不漏应用（位点 > 水位 的事件必被缓冲或应用，且按位点有序）；不重复覆盖
（位点 ≤ 水位 的事件绝不写入目标）。验证见 `tests/test_switch.py`。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
