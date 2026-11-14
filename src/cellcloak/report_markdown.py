"""Markdown rendering for the internal audit report."""

from __future__ import annotations

from typing import Any


def _pct(value: float) -> str:
    return f"{value:.2%}"


def _text(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def render_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# 小域遮审台内部审计报告",
        "",
        "> 内部审计材料。报告有意省略被抑制单元格的原始计数值；原始值仅保存在本地 SQLite 台账。",
        "",
        f"- 数据集：{_text(report['dataset_name'])}",
        f"- 契约版本：{report['contract_version']}",
        f"- 运行 ID：`{report['run_id']}`",
        f"- 状态：`{report['status']}`",
        f"- 开始/结束：{report['started_at']} / {report['finished_at']}",
        f"- 契约 SHA-256：`{report['contract_hash']}`",
        "",
        "## 总览",
        "",
        "| 指标 | 数值 |",
        "| --- | ---: |",
        f"| 表数量 | {summary['table_count']} |",
        f"| 单元格总数 | {summary['cell_count']} |",
        f"| 主抑制 / 互补抑制 | {summary['primary_count']} / {summary['secondary_count']} |",
        f"| 抑制比例 | {_pct(summary['suppression_ratio'])} |",
        f"| 边际/反推核验 | {'通过' if summary['verify_passed'] else '失败'} |",
        f"| 阻断表数量 | {summary['blocked_table_count']} |",
        "",
        "风险类别计数（blocked 以表为单位，其余以源单元格为单位）：",
        "",
    ]
    for label, count in summary["risk_class_counts"].items():
        lines.append(f"- `{label}`: {count}")
    if summary["block_reasons"]:
        lines.extend(["", "## 阻断原因", ""])
        for reason in summary["block_reasons"]:
            lines.append(f"- `{reason['code']}` ({reason['table_id']}): {_text(reason['detail'])}")

    lines.extend(["", "## 输入文件", "", "| 表 | 文件 | SHA-256 |", "| --- | --- | --- |"])
    for item in report["inputs"]:
        lines.append(f"| {_text(item['table_id'])} | {_text(item['source'])} | `{item['sha256']}` |")

    for table in report["tables"]:
        info = table["info_loss"]
        lines.extend(
            [
                "",
                f"## 表 `{table['table_id']}`",
                "",
                f"- 维度：{_text(table['row_dim'])} × {_text(table['col_dim'])}",
                f"- 规模：{table['row_count']} 行 × {table['col_count']} 列，{table['cell_count']} 个单元格",
                f"- 表状态 / 风险级别：`{table['table_status']}` / `{table['risk_level']}`",
                f"- 主抑制 / 互补抑制 / 抑制比例：{table['primary_count']} / "
                f"{table['secondary_count']} / {_pct(table['suppression_ratio'])}",
                f"- 被抑制计数总和占比：{_pct(info['suppressed_count_sum_ratio'])}",
                f"- 最大单行 / 单列抑制比例：{_pct(info['max_row_suppression_ratio'])} / "
                f"{_pct(info['max_col_suppression_ratio'])}",
            ]
        )
        if table["block_reasons"]:
            lines.append("- 阻断原因：" + "; ".join(
                f"`{item['code']}` {_text(item['detail'])}" for item in table["block_reasons"]
            ))
        lines.extend(
            [
                "",
                "### 核验",
                "",
                "| 检查 | 结果 | 证据摘要 |",
                "| --- | --- | --- |",
            ]
        )
        for check in table["verification"]["checks"]:
            lines.append(
                f"| {_text(check['name'])} | {'通过' if check['result'] == 'pass' else '失败'} | "
                f"{_text(check['detail'])} |"
            )
        lines.extend(["", "### 被抑制单元格", "", "| 行 | 列 | 类型 | 风险类别 | 指纹 |", "| --- | --- | --- | --- | --- |"])
        for cell in table["suppressed_cells"]:
            lines.append(
                f"| {_text(cell['row'])} | {_text(cell['col'])} | {cell['suppression_type']} | "
                f"{cell['risk_category']} | `{cell['fingerprint'][:16]}…` |"
            )
        if not table["suppressed_cells"]:
            lines.append("| — | — | — | — | 无 |")
    lines.extend(["", "## 说明", "", "本报告由确定性工程规则生成，不承诺形式化最优抑制或满足全部统计披露控制标准。", ""])
    return "\n".join(lines)
