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

## 跨表共享关系

若同一业务单元格会在多张表中以不同表 ID 再次发布，可在契约中用 `shared_cells` 显式声明别名：

```yaml
shared_cells:
  - name: under18_region_alias
    cells:
      - table: region_by_age
        row: R01
        col: "0-17"
      - table: district_by_age
        row: D07
        col: "0-17"
```

只有被声明关联的表才做联合审查。流水线采用 **“先各表局部求解，再沿共享关系传播并做补抑制闭包”** 策略：

1. 各表按原单表规则求解；未关联表的输出与旧版本完全一致。
2. 关系内任一成员被抑制时，其余成员也在各自表内被抑制（源为主抑制则传播为主抑制，否则为互补抑制）。
3. 接收表重新消除行/列单未知数；新补抑制若命中其他关系成员会继续传播，直至不动点，形成可审计的跨表触发链。
4. 每张表的抑制率仍按自己的 `max_suppression_ratio` 计算；任一成员因联合约束超限，整组阻断（`E_JOINT_GROUP_BLOCKED`），不会出现“每表单独通过”的误判。
5. 已按 `structural` 策略公开为 0 的结构零若与被抑制单元格是同一业务值，会以 `E_JOINT_ALIAS_EXPOSURE` 整组阻断——继续补抑制无法撤回已公开的 0。
6. 独立核验器 `verify_joint` 把别名视为同一未知数，检查别名暴露、关系完整性、逐表比例上限与跨表边际级联重建。

审计报告的 `shared_relations`、`trigger_chain`、`joint_verification` 与真实执行的策略名同步进入 JSON/Markdown/HTML 和 SQLite 台账；`verify --run` 会从台账恢复声明并重新做整组联合核验。

## CLI

```text
python -m cellcloak run --contract <path> --out <dir> [--db <sqlite>] [--json]
python -m cellcloak runs [--limit 20] [--db <sqlite>] [--json]
python -m cellcloak verify --run <run-id> [--db <sqlite>] [--json]
python -m cellcloak diff --run-a <run-id> --run-b <run-id> [--db <sqlite>] [--json]
python -m cellcloak serve --host 127.0.0.1 --port 5017 [--db <sqlite>]
```

`verify` 会从 SQLite 重建单元格矩阵并重新执行独立核验，把结果以 `source = reverify` 追加到 `checks`。失败时写入 `verify_failed`；原本已 `blocked` 的运行保留 `blocked` 状态，但命令仍返回非零。

## 退出码

| 退出码 | 含义 |
| ---: | --- |
| `0` | 成功、列表/对比成功，或核验通过 |
| `1` | 非预期本地运行错误 |
| `2` | 契约/CSV/台账/参数错误 |
| `3` | 流水线完成但被阻断，未生成公开表 |
| `4` | 核验失败 |

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
| `E_SHARED_REFERENCE` / `E_SHARED_COORDINATE` | 共享关系引用未知表/坐标 |
| `E_SHARED_GROUP_DUPLICATE` / `E_SHARED_CELL_DUPLICATE` | 关系名或别名重复 |
| `E_SHARED_CONFLICT` / `E_SHARED_GROUP_SIZE` | 冲突映射或关系成员不足/未跨表 |
| `E_JOINT_SUPPRESSION_RATIO_EXCEEDED` | 跨表传播使某表超过自身抑制率上限（阻断） |
| `E_JOINT_GROUP_BLOCKED` | 关联组内任一表阻断，整组阻断（阻断） |
| `E_JOINT_ALIAS_EXPOSURE` | 公开的结构零与被抑制单元格是同一业务值（阻断） |
| `E_JOINT_SUPPRESSION_MAX_ROUNDS` | 跨表闭包未在轮次上限内稳定（阻断） |
| `E_OUTPUT_NOT_EMPTY` / `E_OUTPUT_MARKER` | 输出目录保护或清单异常 |
| `E_RUN_NOT_FOUND` / `E_RUN_DUPLICATE` | 台账运行不存在/重复 |
| `E_LOOPBACK_ONLY` | 拒绝绑定非回环地址 |

阻断原因与异常错误码分开使用：阻断不是程序失败，而是发布判定。

## 本地页面

`serve` 仅接受 `127.0.0.1`、`localhost` 或 `::1`，拒绝 `0.0.0.0` 与其他主机。页面提供最近运行、运行详情、核验结果与运行对比；“运行演示 fixture”按钮只执行仓库内置 `fixtures/demo`，没有上传、路径输入或远程抓取入口。所有 CSS 本地化，无外部字体、脚本、图片或网络请求。

## 数据合规声明

本项目仅用于处理你本人拥有或已获授权的本地聚合数据。仓库中的所有 fixture 均为合成数据，地区编码 `R01`、`R02` 等均为虚构值，不包含真实个人信息、真实公司名、真实域名或密钥。被抑制原始计数只允许进入本地 SQLite 内部台账；不要把该数据库或内部审计目录当作公开发布物。更多边界见 [`docs/COMPLIANCE.md`](docs/COMPLIANCE.md)。

## 项目结构

```text
src/cellcloak/
  contract.py   契约解析与静态校验（含 shared_cells）
  ingest.py     CSV 读取、矩阵完整性、边际计算
  risk.py       主风险分类
  suppress.py   主抑制、互补抑制、阻断判定
  joint.py      跨表共享关系传播与补抑制闭包
  verify.py     独立边际、反推与跨表联合核验
  report.py     public/ 与 audit/ 产物
  ledger.py     SQLite 台账
  pipeline.py   端到端编排与重核验
  cli.py        命令行
  web.py        仅回环 Flask 页面
```

设计说明见 [`docs/DESIGN.md`](docs/DESIGN.md)。
