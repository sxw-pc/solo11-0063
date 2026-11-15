# 设计说明

## 目标与非目标

Cellcloak 面向**已经聚合的二维计数表**，把发布前审查做成可重复的本地流水线：契约固定规则，算法固定选择顺序，输出分为可公开发布物与内部证据。它刻意不接入分布式求解器、不尝试差分隐私、也不处理个人明细。

## 模块与数据流

```text
YAML contract
   │  contract.load_contract：安全解析、路径约束、静态校验
   ▼
TableSpec ── ingest.load_table ──► TableModel（完整矩阵、边际、输入哈希）
                                      │
                                      ▼
                           risk.classify（主风险标签、主抑制坐标）
                                      │
                                      ▼
                     suppress.suppress_table（主抑制 + 互补抑制）
                                      │
                                      ▼
                     verify.verify_table（独立边际与反推核验）
                                      │
                    ┌─────────────────┴──────────────────┐
                    ▼                                    ▼
       report.emit_outputs                        ledger.Ledger
       public/ 与 audit/                          SQLite 台账
                    │                                    │
                    └──────────── pipeline ──────────────┘
                                 │
                      CLI / loopback Flask UI
```

各阶段只通过不可变或显式数据对象传递结果。核验器不读取抑制器的内部评分或中间状态，而是从矩阵、抑制映射和边际重新计算。

## 模型与稳定性

- `TableModel` 保存声明顺序、完整计数映射、源文件名与 SHA-256。矩阵必须覆盖 `row_order × col_order`；边际总是重新计算。
- 单元格指纹为 `SHA-256("cellcloak-cell-v1", table_id, row, col)`。它**不包含计数**，因此报告和页面可以稳定标识坐标，同时不因指纹暴露小计数值。
- 运行 ID 格式为 `run-<UTC 微秒时间戳>-<随机后缀>`，按字符串可排序；相同输入重复运行会生成新记录，不覆盖旧运行。
- 时间戳使用 UTC ISO 8601；文件统一 UTF-8。

## 主风险分类

对每个单元格：

1. 在 `always_suppress` 中 → `forced`。
2. 计数为 0 且在 `structural` 模式下被声明为结构零 → `safe`。
3. 计数为 0 → `sampling_zero`。
4. `0 < count < min_count` → `small_count`。
5. 其他 → `safe`。

分类前检查声明结构零是否确实为 0。报告与台账使用同一风险标签，但报告只展示标签、计数统计和坐标，不展示被抑制原始值。

## 互补抑制

### 停止条件

一条行/列如果包含恰好一个被抑制单元格，就可以由该行/列的公开合计减去其他可见值直接得到该单元格，因此必须再抑制一个可见单元格。算法反复扫描所有行和列，直到没有任何这样的单元素行/列，或触发阻断。

### 扫描与即时更新

扫描顺序固定为 `row_order` 全部行，然后 `col_order` 全部列。每选择一个补抑制单元格就立即写入抑制映射，因此在同一轮中，一个补抑制单元格可以同时关闭相邻行/列的单未知数状态。这样比“整轮快照后再统一应用”更少产生无谓级联。

### 候选评分

升序排序键：

```text
(count, loss_weight, row_key, col_key)
```

`loss_weight` 来自契约，未声明为 `1.0`。计数较小优先降低被隐藏计数总量；权重是显式的信息损失偏好；最后用行键、列键保证跨平台、跨运行确定性。候选只会来自当前可见单元格。

### 阻断

任一情况出现即判该表 `blocked`：

- 初始主抑制比例已经超过 `max_suppression_ratio`；
- 某单元素行/列没有可见候选；
- 完成一轮后抑制比例超过上限；
- 扫描轮次耗尽 `max_suppression_rounds`。

只要一次运行中任一表 `blocked`，整次运行状态为 `blocked`，不生成任何公开 CSV。审计仍会记录已有抑制映射与原因，便于复核，但不会把局部结果当成可发布表。

### 信息损失摘要

- 被抑制单元格数量与比例；
- 被抑制计数总和占全表计数总和比例；
- 最大单行抑制比例；
- 最大单列抑制比例。

这些是聚合统计，不包含单格原始值。

## 独立核验

`verify_table` 只读取矩阵、抑制映射、主抑制坐标和比例上限：

1. **边际一致性**：重算每行、每列与总计，并校验三组汇总相等。
2. **抑制映射有效性**：坐标必须存在，类型必须是 `primary` 或 `secondary`。
3. **主抑制覆盖**：契约风险识别出的每个主抑制坐标必须仍被抑制。
4. **比例上限**：重新计算抑制比例。
5. **单未知数行/列**：独立统计恰好一个抑制单元格的行和列。
6. **重建攻击模拟**：仅根据公开边际关系，反复寻找“某行/列只剩一个未知数”的路径并级联消元；任何被恢复的抑制单元格都失败。
7. **逐单元格证据**：坐标、指纹、抑制类型、风险类别、行/列抑制数、是否可直接推导。

运行状态判定顺序：存在阻断表 → `blocked`；否则核验失败 → `verify_failed`；否则 `ok`。核验失败不生成公开表。

`verify --run` 从单一 SQLite 读事务重建矩阵与抑制映射、计算规范输入快照 SHA，并打开一条不可变 **verify attempt** 审计记录后再运行同一独立核验。封存时在**单个事务**内写入整组 checks、attempt 结果/结束时间与 `runs.status` 投影，并在提交前重新计算快照 SHA：

- 每次 `verify` 都新建 attempt（`source=reverify`），初始建仓核验作为 `source=run` 的 attempt；历史 attempt 与 checks 永不覆盖。
- attempt 状态机：`interrupted`（已开启但封存前中断，可见且不伪装完成）→ `completed`（`result=pass/fail`）或 `stale`（读取与封存之间 run/table/cell 事实发生变化，不投影状态并返回可重试错误）。
- `runs.status` 只是最新一条快照仍一致的已完成 attempt 的投影：失败时 `blocked` 保持 `blocked`，否则写 `verify_failed`；通过时 `blocked` 保持 `blocked`，其他状态回到 `ok`。
- 封存事务中任何异常都会整体回滚：上一条 completed attempt、其 checks 与 `runs.status` 保持一致，中断的 attempt 留为 `interrupted`。
- 旧库打开时自动迁移：历史 `source='reverify'` checks 收编为一条 `source=legacy` 的已完成 attempt，证据不丢失。
- `diff` 只比较两次运行的当前 status、表格与公开产物，attempt 数量不计入数据差异。

## 报告与发布

- `public/<table_id>.csv` 仅对 `ok` 运行生成。被抑制单元格写为 `NA`，保留行/列/总合计，供下游发布分析使用。由于所有单元素行/列已消除，这些合计不能直接反推任一抑制值。
- `audit/report.json` 是机器可读的完整证据；`report.md` 与自包含 `report.html` 供人工审阅。HTML 的所有样式内联，不引用外部脚本、字体、样式或图片。
- 报告不包含抑制单元格原始计数；原始计数只进入本地 SQLite `cells` 表。报告中的输入哈希、风险类别、计数统计与指纹用于复核。
- 输出目录带 `.cellcloak-output.json` 清单。工具只清理清单中记录的自有文件；遇到无清单但已有 `public/` 或 `audit/` 内容的目录会拒绝覆盖。

## SQLite 台账

| 表 | 用途 |
| --- | --- |
| `runs` | 运行 ID、数据集、状态、开始/结束、契约哈希、输出目录、汇总 JSON |
| `tables` | 表维度、主/补抑制数、比例、状态、阻断原因、元数据 |
| `cells` | 每个单元格的原始计数、抑制类型、风险类别、指纹；**仅内部** |
| `verify_attempts` | 不可变核验尝试：attempt ID、序号、来源（run/reverify/legacy）、状态、结果、输入快照 SHA、开始/结束时间 |
| `checks` | 每项核验结果与证据摘要，经 `attempt_pk` 归属到具体 attempt |
| `artifacts` | 产物路径、类型、SHA-256 |

台账连接在每次操作中创建并提交，启用外键；所有写操作使用 SQLite 事务。`runs` 与 `verify_attempts` 不覆盖历史记录；attempt 的 checks、结果封存与状态投影在同一事务内同成同败。`diff` 比较抑制比例、主/补抑制、阻断状态、表状态与公开 CSV 哈希，不比较 attempt 数量。

## Web

Flask 应用只由 CLI 在回环地址启动。页面数据均来自本地台账；不展示 `cells` 原始计数。运行详情页按 attempt 分组展示每条核验尝试的开始/结束时间、`pass/fail/stale/interrupted` 状态与 checks，初始 `source=run` 核验与各次 reverify attempt 分开陈列。演示按钮调用同一个 `execute_run`，契约固定为仓库内置 `fixtures/demo/contract.yml`，没有上传或远程抓取入口。响应带 CSP、`X-Frame-Options: DENY`、`nosniff` 等本地安全头。

## 确定性、边界与限制

- 不依赖随机抑制；随机性只用于运行 ID 后缀和 Flask 会话密钥。
- 不保证抑制集合基数最小，也不保证联合边际、复杂整数约束或 3 维表的安全性。
- 单未知数核验不等于完整统计披露控制；发布者仍需结合抽样设计、表间关联、上下文信息和组织政策审查。
- 数据规模按内存中的二维表设计。若表非常宽或非常大，应在外部先做工程评估。
- 当前版本不支持同一契约内多张表之间的交叉推导。
