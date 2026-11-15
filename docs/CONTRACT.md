# 发布契约规范（version 1）

契约是 YAML 文件，由 `yaml.safe_load` 读取，必须以 UTF-8 或 UTF-8-SIG 编码。契约同时约束输入 CSV 的形状与披露规则。运行时完全离线。

## 完整字段表

### 顶层

| 字段 | 类型 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `version` | 整数 | 是 | — | 目前只支持 `1`，其他值返回 `E_VERSION_UNSUPPORTED` |
| `dataset_name` | 非空字符串 | 是 | — | 合成数据集名称；不得写入真实个人信息 |
| `privacy` | 映射 | 是 | — | 披露策略 |
| `tables` | 非空列表 | 是 | — | 一张或多张二维计数表声明 |

### `privacy`

| 字段 | 类型 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `min_count` | 整数 ≥ 1 | 否 | `5` | `0 < count < min_count` 进入主抑制 |
| `zero_policy` | 枚举 | 是 | — | `structural` 或 `protect_sampling` |
| `max_suppression_ratio` | 数值 `[0,1]` | 否 | `0.35` | 被抑制单元格数 / 表单元格总数上限；超过即阻断 |
| `structural_zero_cells` | 单元格引用列表 | 否 | `[]` | 允许公开为 0 的结构零；必须确实为 0 |
| `always_suppress` | 单元格引用列表 | 否 | `[]` | 强制抑制单元格，即使计数不低于阈值或为零 |
| `max_suppression_rounds` | 整数 `[1,100000]` | 否 | `100` | 互补抑制最大扫描轮次 |

`zero_policy` 语义：

- `structural`：只有同时出现在 `structural_zero_cells` 中的 0 可公开为 0；其他 0 标为 `sampling_zero` 并被主抑制。
- `protect_sampling`：所有 0 都标为 `sampling_zero` 并被主抑制。即使同时写了 `structural_zero_cells`，也不会把 0 当作可公开结构零；但被引用的单元格仍必须是 0。

`always_suppress` 优先级最高。若一个结构零同时被强制抑制，其风险类别是 `forced`。

### `tables[]`

| 字段 | 类型 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | 字符串 | 是 | — | 唯一表 ID；匹配 `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`，亦用于公开文件名 |
| `source` | 相对路径 | 是 | — | CSV 相对契约目录的路径；禁止绝对路径、`..` 越界与目录逃逸 |
| `row_dim` | 非空字符串 | 是 | — | CSV 行维列名 |
| `col_dim` | 非空字符串 | 是 | — | CSV 列维列名 |
| `value` | 非空字符串 | 是 | — | CSV 计数值列名 |
| `row_order` | 非空不重复字符串列表 | 是 | — | 行键完整顺序；也决定候选扫描顺序 |
| `col_order` | 非空不重复字符串列表 | 是 | — | 列键完整顺序 |
| `missing_cell_policy` | 枚举 | 否 | `error` | `error` 或 `zero`；决定缺失组合是否补零 |
| `loss_weights` | 权重列表 | 否 | `[]` | 互补抑制候选的次要排序依据；未声明权重视为 `1.0` |

`row_dim`、`col_dim`、`value` 必须互不相同。表 ID 不得重复。

### 单元格引用

`structural_zero_cells[]` 与 `always_suppress[]` 的每一项：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `table` | 字符串 | 是 | 必须引用已声明表 |
| `row` | 字符串 | 是 | 必须在目标的 `row_order` 中 |
| `col` | 字符串 | 是 | 必须在目标的 `col_order` 中 |

同一列表内不得重复引用同一单元格。引用不存在的表返回 `E_CONTRACT_REFERENCE`；坐标不在声明顺序中返回 `E_CELL_REFERENCE`。

### `loss_weights[]`

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `row` | 字符串 | 是 | 行键 |
| `col` | 字符串 | 是 | 列键 |
| `weight` | 非负有限数值 | 是 | 信息损失权重；越小越优先被选为互补抑制 |

权重只在计数相同时参与候选排序。候选的完整排序键是 `(计数, 权重, 行键, 列键)`，均为升序，从而保证确定性。

## 完整示例

```yaml
version: 1
dataset_name: synthetic_demo_2026
privacy:
  min_count: 5
  zero_policy: structural
  max_suppression_ratio: 0.35
  max_suppression_rounds: 100
  structural_zero_cells:
    - table: age_by_sex
      row: U
      col: "65+"
  always_suppress: []
tables:
  - id: age_by_sex
    source: age_by_sex.csv
    row_dim: sex
    col_dim: age_band
    value: count
    row_order: [F, M, U]
    col_order: ["0-17", "18-34", "35-64", "65+"]
    missing_cell_policy: error
  - id: region_by_age
    source: region_by_age.csv
    row_dim: region
    col_dim: age_band
    value: count
    row_order: [R01, R02, R03, R04]
    col_order: ["0-17", "18-34", "35-64", "65+"]
    missing_cell_policy: error
    loss_weights:
      - row: R02
        col: "65+"
        weight: 0.5
```

## CSV 规则

- 长格式：每行一个 `(row_dim, col_dim, value)` 组合。
- 必须存在 `row_dim`、`col_dim`、`value` 三列；表头重复、行字段数超出表头都会报错。
- 行/列键会去除首尾空白；必须精确出现在对应顺序列表中。
- 计数值会去除首尾空白，只接受 ASCII 十进制非负整数；不接受 `-1`、`1.0`、`+1`、科学计数法或空值。
- 完全空行被忽略。支持 UTF-8 与 UTF-8-SIG，忽略开头的空行。
- 重复单元格报 `E_CSV_DUPLICATE`。
- 默认 `missing_cell_policy: error` 要求完整笛卡尔积；`zero` 会为缺失组合补 0。

## 静态与运行期校验

**加载契约时**

- YAML 安全解析；禁止自定义构造器。
- 版本、类型、枚举、边界、重复表 ID、绝对/越界源路径、源文件存在性。
- 表维度名互异、顺序列表非空且唯一。
- 所有单元格引用在已声明表与顺序内。

**加载 CSV / 分类时**

- 表头、编码、整数计数、重复与缺失组合。
- 声明的结构零必须实际为 0，否则返回 `E_STRUCTURAL_ZERO_VALUE`。

## 风险标签

| 标签 | 层级 | 含义 |
| --- | --- | --- |
| `safe` | 单元格 | 通过主规则，或已声明且真实的 0 |
| `small_count` | 单元格 | `0 < count < min_count` |
| `sampling_zero` | 单元格 | 未被允许公开的 0 |
| `forced` | 单元格 | 由 `always_suppress` 强制 |
| `blocked` | 表 / 运行 | 抑制无法在契约约束内完成 |

`blocked` 在汇总中按表计数，其余标签按源单元格计数；审计报告有显式说明。

## 常见错误码

| 错误码 | 触发点 |
| --- | --- |
| `E_CONTRACT_READ` / `E_CONTRACT_ENCODING` | 文件不可读/非 UTF-8 |
| `E_CONTRACT_YAML` | YAML 解析失败或使用不安全标签 |
| `E_CONTRACT_SCHEMA` | 字段类型、范围、必填项错误 |
| `E_VERSION_UNSUPPORTED` | `version != 1` |
| `E_TABLE_DUPLICATE` / `E_TABLE_UNKNOWN` | 表 ID 重复/查询未知表 |
| `E_SOURCE_PATH` / `E_SOURCE_MISSING` | 源路径越界或不存在 |
| `E_CONTRACT_REFERENCE` / `E_CELL_REFERENCE` | 引用未知表/未知坐标 |
| `E_CSV_*` | CSV 读取、表头、值、重复或缺失单元格错误 |
| `E_CSV_SOURCE_CHANGED` | 已核验字节在解析前摘要不再匹配 |
| `E_STRUCTURAL_ZERO_VALUE` | 声明的结构零不是 0 |
| `E_MANIFEST_*` | 输入集 manifest 读取、签名、契约绑定、成员或快照错误（详见 README） |

错误对象包含稳定的 `code`，并尽可能附带 `table_id`、`field`、`cell` 与文件定位信息。CLI 只打印稳定错误码与消息，不打印堆栈。

## 输入集 manifest

manifest 不是契约的一部分，而是对一次输入捕获的独立承诺。`cellcloak manifest --contract <path> --out <file>` 的输出结构：

```yaml
version: 1
kind: cellcloak-input-manifest
dataset_name: synthetic_demo_2026
contract_hash: <规范化契约 SHA-256>
members:
  - table_id: age_by_sex
    source: age_by_sex.csv
    sha256: <原始 CSV 字节 SHA-256>
    size: 140
manifest_sha256: <上述内容的规范化 JSON SHA-256>
```

`contract_hash` 是规范化语义 hash，不是 YAML 文件 hash；仅调整格式、注释或键顺序不会改变它，而修改隐私策略、表顺序、维度、顺序列表、权重或相对 source 会改变它。`manifest_sha256` 对除自身之外的全部字段做规范化 JSON 摘要，读取时重算校验。成员必须与契约 `tables` 顺序一致，不允许缺失、重复、重排、额外表或越界/绝对 source 路径。
