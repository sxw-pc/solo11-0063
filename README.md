# 小域遮审台（Cellcloak）

一个完全离线、可测试、可复现的本地工具：在发布**已聚合的二维计数表**之前，按 YAML 契约识别小计数单元格，执行主抑制与互补抑制，独立核验行/列边际不可直接反推，并输出可发布遮蔽表、内部审计报告与 SQLite 台账。

## 适用与不适用

**适用**

- 小团队发布“地区 × 年龄段”等二维计数交叉表前的本地预检。
- 需要固定、可解释、可回归的 `min_count` 主抑制与确定性互补抑制规则。
- 需要保留内部审计证据，但默认不在报告中明示被抑制原始小计数的场景。

**不适用**

- 不处理个人明细、字段级 PII 识别或行级脱敏。
- 不实现差分隐私、合成数据生成或匿名化算法研究。
- 不是形式化最优抑制求解器；不依赖外部求解器，也不承诺满足全部统计披露控制标准。
- 不是 BI 看板、报表平台、权限后台、湖仓版本管理、SQL 血缘或指标口径平台。
- 不处理 3 维及以上表格，不处理连续变量、加权估计或复杂抽样设计。

## 快速上手

先安装本地依赖与包（离线环境请预先准备 Flask、PyYAML、pytest）：

```bash
python -m pip install -e .
```

三条日常命令：

```bash
# 1. 跑演示流水线
python -m cellcloak run --contract fixtures/demo/contract.yml --out out/demo

# 2. 启动仅监听本机的页面
python -m cellcloak serve --host 127.0.0.1 --port 5017

# 3. 跑测试
pytest -q
```

演示运行成功时输出 `status: ok`。阻断 fixture 可用：

```bash
python -m cellcloak run --contract fixtures/blocked/contract.yml --out out/blocked
```

命令会返回退出码 `3`，并且 `out/blocked/public/` 中不会生成可发布 CSV。

## 输入契约

完整字段、校验规则与示例见 [`docs/CONTRACT.md`](docs/CONTRACT.md)。最小示例：

```yaml
version: 1
dataset_name: synthetic_demo_2026
privacy:
  min_count: 5
  zero_policy: structural
  max_suppression_ratio: 0.35
  structural_zero_cells:
    - table: age_by_sex
      row: U
      col: "65+"
tables:
  - id: region_by_age
    source: region_by_age.csv
    row_dim: region
    col_dim: age_band
    value: count
    row_order: [R01, R02, R03, R04]
    col_order: ["0-17", "18-34", "35-64", "65+"]
    missing_cell_policy: error
```

CSV 为长格式，所需列为契约中的行维度、列维度与计数值。计数值必须是非负整数；默认要求 `row_order × col_order` 全覆盖，`missing_cell_policy: zero` 时缺失组合补零。读取支持 UTF-8 与 UTF-8-SIG。

## 输出目录

```text
out/demo/
├── public/
│   ├── age_by_sex.csv
│   └── region_by_age.csv
├── audit/
│   ├── report.json
│   ├── report.md
│   └── report.html
└── .cellcloak-output.json
```

- `public/`：仅当整次运行状态为 `ok` 时生成。被抑制单元格写为 `NA`，同时保留行合计、列合计与总合计。不会写入被抑制原始计数。
- `audit/`：内部审计材料。包含契约与输入文件哈希、运行 ID、风险分类、抑制统计、阻断原因、边际/反推核验证据、被抑制单元格坐标与指纹、信息损失摘要。报告默认不展示被抑制原始值。
- `.cellcloak-output.json`：工具自用的输出清单，只清理自己上次写入的文件，避免阻断运行时遗留旧公开表。
- SQLite 台账默认位于 `var/cellcloak.sqlite3`，可用 `--db` 或 `CELLCLOAK_DB` 指向别处。`cells` 表保存原始计数，仅限内部使用。

## 抑制规则

1. **主抑制**：`0 < count < min_count` 的单元格标记为 `small_count`；零值按 `zero_policy` 处理；`always_suppress` 强制抑制，即使计数较大；`structural_zero_cells` 中且确为零的单元格可公开为 0。
2. **互补抑制**：按声明顺序扫描每一行与每一列。若某行/列中当前恰好只有一个被抑制单元格，则必须再抑制一个可见单元格。
3. **候选评分**：先选计数更小的单元格，再选契约 `loss_weights` 中权重更小的单元格，最后按 `(行键, 列键)` 字典序打破平局。选择即时生效，使一个补抑制单元格能同时关闭相邻的行和列单未知数。
4. **停止与阻断**：重复扫描直到没有单元素可推导行/列；若抑制比例超过 `max_suppression_ratio`、无可选候选、或轮次超限，则整批判定为 `blocked`，不生成公开表。
5. **独立核验**：核验器不信任抑制过程，单独重算边际，并利用公开行/列合计与可见值进行单未知数级联攻击模拟；任何可解路径都判失败。

这是工程启发式，不是形式化最优解，也不等于差分隐私。它会优先降低被隐藏的计数总量，但可能牺牲比理论最优更多的单元格。

## CLI

```text
python -m cellcloak run --contract <path> --out <dir> [--db <sqlite>] [--json]
python -m cellcloak runs [--limit 20] [--db <sqlite>] [--json]
python -m cellcloak verify --run <run-id> [--db <sqlite>] [--json]
python -m cellcloak diff --run-a <run-id> --run-b <run-id> [--db <sqlite>] [--json]
python -m cellcloak forget --run <run-id> --reason <code> [--dry-run] [--db <sqlite>] [--json]
python -m cellcloak serve --host 127.0.0.1 --port 5017 [--db <sqlite>]
```

`verify` 会从 SQLite 重建单元格矩阵并重新执行独立核验，把结果以 `source = reverify` 追加到 `checks`。失败时写入 `verify_failed`；原本已 `blocked` 的运行保留 `blocked` 状态，但命令仍返回非零。

## 数据保留与清除（forget）

原始计数长期保存在本地台账 `cells` 表中。保留期结束后，用 `forget` 删除指定 run 的敏感数据：

```bash
# 先看不修改任何内容的清单
python -m cellcloak forget --run <run-id> --reason retention-2026 --dry-run

# 实际清除
python -m cellcloak forget --run <run-id> --reason retention-2026
```

清除内容：

- SQLite 中该 run 的 `runs`/`tables`/`cells`/`checks`/`artifacts` 行（级联删除，含全部原始计数、坐标键与内部报告路径）；
- 该 run 登记的内部报告文件 `audit/report.json`、`report.md`、`report.html`。

`public/*.csv` 不含原始计数，属于可发布物，默认保留。`--reason` 是操作者给出的原因码（1–64 位字母数字及 `. _ : -`），会记入墓碑。

**文件删除是保守的。** 每个候选文件必须同时满足：在台账中登记于该 run、被输出目录的 `.cellcloak-output.json` 清单确认、解析后仍在该 run 的输出目录内、不是符号链接、未被其他保留 run 的台账记录引用、且当前磁盘 SHA-256 与该 run 登记的哈希一致。任一不满足（后来的 run 覆盖了同目录文件、外部改写、marker 损坏或缺失、路径逃逸、符号链接）都会保留该文件并在结果中给出具体原因（如 `kept_hash_mismatch`、`kept_shared_reference`、`kept_marker_invalid`）。

**清除后保留最小审计墓碑**：`run_id`、删除时间、原因码与聚合删除结果（不含原始计数、坐标键和文件路径）。因此 `runs` 仍会列出该 run（状态 `purged`），`verify` 明确返回 `purged`/`purging` 而不是假装它从未存在，`diff` 会拒绝比较已清除的 run。

**生命周期可恢复**（路线 A）：先事务性写入 `purging` 墓碑，再逐项核对并删除文件，最后在同一事务内级联删除台账行并把墓碑标为 `purged`。任何一步中断后重跑同一命令即可安全续做；中途失败时 `verify` 返回 `purging`。重复执行已完成的清除是幂等空操作。

退出码：`0` 清除完成且无保留文件（或 dry-run、已清除）；`5` 清除完成但有文件被保留，需人工跟进（重复执行时若结果仍含保留文件同样返回 `5`）；`2` 参数或台账错误。

## 退出码

| 退出码 | 含义 |
| ---: | --- |
| `0` | 成功、列表/对比成功，或核验通过 |
| `1` | 非预期本地运行错误 |
| `2` | 契约/CSV/台账/参数错误 |
| `3` | 流水线完成但被阻断，未生成公开表 |
| `4` | 核验失败（含对已清除 run 执行 `verify`） |
| `5` | `forget` 完成但部分文件被保留 |

## 常见错误码

| 错误码 | 含义 |
| --- | --- |
| `E_VERSION_UNSUPPORTED` | 契约版本不是 `1` |
| `E_CONTRACT_SCHEMA` / `E_CONTRACT_YAML` | 契约结构或 YAML 非法 |
| `E_TABLE_DUPLICATE` | 表 ID 重复 |
| `E_SOURCE_PATH` / `E_SOURCE_MISSING` | CSV 路径越界或不存在 |
| `E_CELL_REFERENCE` | 强制抑制/结构零/权重引用了不存在的坐标 |
| `E_CSV_HEADER` | 缺少必需列、重复表头或多余字段 |
| `E_CSV_VALUE` | 计数为负、小数、空值或非整数 |
| `E_CSV_DUPLICATE` | 重复单元格 |
| `E_CSV_MISSING_CELL` | 默认策略下矩阵不完整 |
| `E_STRUCTURAL_ZERO_VALUE` | 声明的结构零在源表中并非 0 |
| `E_SUPPRESSION_RATIO_EXCEEDED` | 抑制比例超过契约上限 |
| `E_SUPPRESSION_NO_CANDIDATE` | 单元素行/列已无可见单元格可补抑制 |
| `E_SUPPRESSION_MAX_ROUNDS` | 互补抑制未在轮次上限内稳定 |
| `E_OUTPUT_NOT_EMPTY` / `E_OUTPUT_MARKER` | 输出目录保护或清单异常 |
| `E_RUN_NOT_FOUND` / `E_RUN_DUPLICATE` | 台账运行不存在/重复 |
| `E_RUN_PURGED` / `E_RUN_PURGING` | 运行原始记录已清除/正在清除，仅剩审计墓碑 |
| `E_FORGET_REASON` | `forget --reason` 原因码格式非法 |
| `E_LOOPBACK_ONLY` | 拒绝绑定非回环地址 |

阻断原因与异常错误码分开使用：阻断不是程序失败，而是发布判定。

## 本地页面

`serve` 仅接受 `127.0.0.1`、`localhost` 或 `::1`，拒绝 `0.0.0.0` 与其他主机。页面提供最近运行、运行详情、核验结果与运行对比；“运行演示 fixture”按钮只执行仓库内置 `fixtures/demo`，没有上传、路径输入或远程抓取入口。所有 CSS 本地化，无外部字体、脚本、图片或网络请求。

## 数据合规声明

本项目仅用于处理你本人拥有或已获授权的本地聚合数据。仓库中的所有 fixture 均为合成数据，地区编码 `R01`、`R02` 等均为虚构值，不包含真实个人信息、真实公司名、真实域名或密钥。被抑制原始计数只允许进入本地 SQLite 内部台账；不要把该数据库或内部审计目录当作公开发布物。更多边界见 [`docs/COMPLIANCE.md`](docs/COMPLIANCE.md)。

## 项目结构

```text
src/cellcloak/
  contract.py   契约解析与静态校验
  ingest.py     CSV 读取、矩阵完整性、边际计算
  risk.py       主风险分类
  suppress.py   主抑制、互补抑制、阻断判定
  verify.py     独立边际与反推核验
  report.py     public/ 与 audit/ 产物
  ledger.py     SQLite 台账（含清除墓碑）
  forget.py     保留期清除生命周期与文件安全核验
  pipeline.py   端到端编排与重核验
  cli.py        命令行
  web.py        仅回环 Flask 页面
```

设计说明见 [`docs/DESIGN.md`](docs/DESIGN.md)。
