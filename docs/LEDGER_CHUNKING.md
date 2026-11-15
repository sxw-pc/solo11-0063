# 台账分块入账与封存协议

本文档描述 SQLite 台账的有界分块写入协议（schema v1）、崩溃恢复流程，以及
与"单事务分块写入"替代方案的测量对比和选型建议。

## 背景问题

改造前的写入路径有两处随单元格总数线性增长的内存占用：

1. `pipeline._ledger_payload` 把每个 `model.cells()` 追加成完整的 `cell_records` dict 列表；
2. `ledger.record_run` 再把所有 cell dict 复制成第二份完整 tuple 列表供 `executemany`。

对宽表（百万格）这意味着两份 O(R×C) 的 Python 对象同时驻留。此外，整个
runs/tables/cells/checks/artifacts 写入包在一个事务里，只有连接退出时才提交：
写锁贯穿全程，且崩溃后不留任何可恢复状态。

## 协议状态机

```text
            begin_run                stream_cells / write_cell_chunk          seal_run
  (不存在) ───────────► recording ───────────────────────────────► recording ──────────► sealed
                            │                          ▲                                （唯一可见态）
                            │ crash                    │ 幂等跳过已提交块
                            ▼                          │
                     recording（重启后可见于                abort_run
                     `ledger pending`，可 resume） ───────────────► （整行级联删除）
```

- `sealed = 0`（recording）：对外不可见。`list_runs`、`get_run_detail`、
  `load_for_verification` 一律过滤，效果等同于 run 不存在——绝不会把某个
  chunk 的部分矩阵或 0 格矩阵暴露给核验流程。
- `sealed = 1`：完整、不可变的审计记录。封存后拒绝一切单元格写入
  （`E_RUN_SEALED`）与 `abort`。

## 阶段一：begin_run（一个小事务）

提交 run 行（`sealed = 0`）、全部 tables/checks/artifacts 行，以及封存期望
`seal_expect_json`：

```json
{
  "cell_count": 1000000,
  "digest": "<链式坐标摘要>",
  "table_count": 2,
  "check_count": 14,
  "artifact_count": 5,
  "chunk_size": 4096
}
```

`cell_count` 与 `digest` 来自流水线对规范单元格流（逐表 × 行序 × 列序）的
第一遍流式折叠，额外内存为 O(chunk)，不产生单元格列表。

## 阶段二：分块写入（每块一个短事务）

每块（默认 4096 格）携带：

| 字段 | 含义 |
| --- | --- |
| `chunk_seq` | 稳定序号，从 0 连续递增；乱序写入被 `E_LEDGER_CHUNK_SEQ` 拒绝 |
| `cell_count` | 本块坐标数 |
| `chunk_digest` | 本块有序坐标/计数序列的 SHA-256（长度前缀编码，含表名、行键、列键、raw_count、抑制类型、风险类别） |
| `chain_digest` | `H(前一块 chain_digest ‖ 本块 chunk_digest)`；创世值绑定 `run_id`，跨 run 的块无法通过链校验 |

- 块行与其 cells 在**同一事务**提交：块中崩溃 → 两者一起回滚，不留半块。
- 重复写入同序号同内容的块是**幂等**的（返回 `duplicate`，不重复插入）；
  同序号不同内容被 `E_LEDGER_CHUNK_CONFLICT` 明确拒绝。
- `chunk_size` 在 `begin_run` 时固化进封存期望，续传时强制沿用，保证块边界
  与摘要可复现。

## 阶段三：seal_run（一个核验事务）

只有在同一事务内全部通过后才 `sealed = 1`：

1. 块序号连续（`COUNT == MAX(seq) + 1`）；
2. 累计数量：块 `cell_count` 之和 == 期望 == `cells` 实际行数 == tables 的
   `cell_count` 之和；
3. 坐标序列摘要：按序重放全部 `chunk_digest` 得到的链终值 == 期望摘要，
   且每块存储的 `chain_digest` 与重算值一致；
4. 关联完整：tables/checks/artifacts 行数符合期望，且不存在不属于本 run
   任何表的孤儿 cells。

任一失败整体回滚（`E_LEDGER_SEAL`），run 保持 recording，可修复后重试。

## 崩溃恢复

```bash
cellcloak ledger pending            # 列出未封存的 recording run 及进度
cellcloak ledger resume --run <id>  # 确定性重放流水线，校验并跳过已提交块，补写剩余块后封存
cellcloak ledger abort  --run <id>  # 删除未封存 run，FK 级联清掉全部暂存行
```

- `resume` 从台账记录的 `contract_path` 重新执行流水线；契约哈希、run 状态、
  各表行列/抑制计数、以及重放的链式摘要必须与封存期望一致，否则以
  `E_LEDGER_RESUME` 拒绝（提示 abort 后重跑）。已提交的块逐块比对摘要，
  内容漂移会以 `E_LEDGER_CHUNK_CONFLICT` 暴露——这就是"混入其他 run 的块"
  的检测点；直接篡改 `run_chunks.run_id` 也会在封存时被 run 绑定的链式摘要
  识破。
- 已封存的 run 不可 resume、不可 abort（`E_RUN_SEALED`）。

## 失败模式矩阵（均有测试覆盖）

| 场景 | 行为 |
| --- | --- |
| 块中崩溃（executemany 中途失败） | 块行与 cells 同事务回滚；`pending` 可见；可续传 |
| 块后崩溃（已提交 k 块） | 重启后 `pending` 显示进度 k/N；读接口不可见；resume 幂等续传 |
| 重复块（同内容） | 幂等跳过，不重复插入 |
| 重复块（不同内容） | `E_LEDGER_CHUNK_CONFLICT` |
| 乱序块 | `E_LEDGER_CHUNK_SEQ` |
| 摘要失配 / 缺块 / 缺 cells / 子表关联断裂 | `E_LEDGER_SEAL`，保持未封存 |
| 混入其他 run 的块 | 续传时逐块摘要比对拒绝；封存时 run 绑定链校验拒绝 |
| 录制中并发 list/verify | 读者只见已封存 run；seal 原子翻转可见性 |
| 封存后写入 / abort | `E_RUN_SEALED` |
| 旧库（v0）打开 | 自动迁移：补列、`sealed` 回填 1、建 `run_chunks`、`user_version = 1` |

## 公开接口不泄露内部状态

`list_runs` / `get_run_detail` 使用显式列清单（不再 `SELECT *`），`sealed`、
`seal_expect_json`、`sealed_at`、`run_chunks` 与 `cells.raw_count` 不会出现在
任何公开 run 详情中；`load_for_verification` 仍是唯一的内部含原始计数入口，
且只接受已封存 run。

## 路径对比测量

两条候选路径都用生成器消除了双列表（峰值行缓冲固定为一块），差别在事务
边界与恢复语义。测量方法见 `benchmarks/bench_ledger.py`（chunk = 4096，
读探针在写入期间持续轮询一个已封存 run；每组在独立子进程中测量，
journal 列区分 WAL 与传统回滚日志）：

| cells | 路径 | journal | 耗时 s | 峰值 Python MB | 峰值 RSS MB | WAL 峰值 MB | 完成读次数 | 读均值 ms | 读 p95 ms | 读 max ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 100,000 | A | wal | 3.52 | 2.3 | 30.4 | 15.3 | 34,114 | 0.08 | 0.15 | 18.5 |
| 100,000 | B | wal | 5.72 | 5.0 | 35.3 | 4.6 | 36,769 | 0.13 | 0.14 | 33.1 |
| 100,000 | A | delete | 1.53 | 1.4 | 25.8 | — | 2,738 | 0.54 | 0.17 | **1,199** |
| 100,000 | B | delete | 5.73 | 5.0 | 35.1 | — | 35,959 | 0.13 | 0.15 | 29.2 |
| 1,000,000 | A | wal | 33.53 | 10.1 | 68.3 | **153.1** | 300,566 | 0.09 | 0.15 | 30.8 |
| 1,000,000 | B | wal | 57.27 | 14.5 | 77.2 | 4.8 | 345,418 | 0.14 | 0.14 | 46.8 |
| 1,000,000 | A | delete | 13.38 | 1.4 | 25.9 | — | 2,613 | 5.11 | 0.17 | **13,048** |
| 1,000,000 | B | delete | 58.25 | 14.5 | 77.2 | — | 341,947 | 0.14 | 0.15 | 44.8 |

读数解读：

- **峰值内存**：两条路径都与总格数无关（10 万 → 100 万格基本持平），
  旧实现的两份全量列表在百万格下会是数百 MB 级。B 略高（一块 dict +
  摘要状态），仍在十几 MB 内。
- **读写阻塞**：WAL 下两条路径的读者都几乎不被阻塞；但回滚日志下
  路径 A 的单事务会让读者**最长等 13 秒**（百万格），而路径 B 的读者
  只排在毫秒级的块事务后面（max ≈ 45 ms）。完成读次数（2,613 vs
  341,947）直观展示了阻塞差异。
- **WAL 峰值**：路径 A 把整个加载先堆进 WAL（百万格 ≈ 153 MB，提交前
  无法 checkpoint）；路径 B 在块间自动 checkpoint，稳定在 ≈ 5 MB。
- **写入耗时**：B 约为 A 的 1.6–4 倍（摘要双遍流 + 每块一次提交），
  这是可恢复性的固定代价。

### 结论：什么条件下选哪一路

- **路径 A（单事务分块 executemany）**：实现最简、全部或全无、写入最快。
  适合**单元格 ≤ 约 10 万、单 writer、写入期间没有并发读取诉求、崩溃后
  可接受整体重跑**的场景。注意即使在 WAL 下，它的 WAL 文件也会随加载量
  线性膨胀，且一旦退到回滚日志模式，读者会被整体阻塞至提交。
- **路径 B（分块短事务 + 隐藏 recording + 末尾封存）**：读者在任何日志
  模式下都只受毫秒级块事务影响；WAL 占用有界；崩溃后可续传而非重跑。
  代价是写入吞吐下降与协议复杂度（读接口过滤、唯一键、链式摘要、封存
  核验、恢复命令）。适合**单元格 ≥ 数十万、写入期间有并发读取（本地
  Web UI）、加载时间长而重跑代价高**的场景。

本项目选择路径 B：它同时满足有界内存、读接口零部分可见、崩溃可恢复三项
硬性要求；路径 A 的"接口简单"无法替代续传能力（崩溃即全量重跑），而
"generator + 单事务"只解决双列表、不缩短写锁——两者不是同等安全的实现。
