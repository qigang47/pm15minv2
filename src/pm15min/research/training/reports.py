from __future__ import annotations

import pandas as pd


def render_offset_training_report(
    *,
    offset: int,
    rows: int,
    positive_rate: float,
    feature_count: int,
    dropped_features,
    metrics: dict[str, dict[str, float | None]],
    explainability: dict[str, object] | None = None,
) -> str:
    dropped = ", ".join(dropped_features) if dropped_features else "(none)"
    lines = [
        f"# Training Offset {int(offset)}",
        "",
        f"- rows: `{int(rows)}`",
        f"- positive_rate: `{positive_rate:.6f}`",
        f"- feature_count: `{int(feature_count)}`",
        f"- dropped_features: `{dropped}`",
        "",
        "## Metrics",
        "",
        "| model | brier | logloss | auc |",
        "| --- | ---: | ---: | ---: |",
        *[
            f"| {name} | {values.get('brier')} | {values.get('logloss')} | {values.get('auc')} |"
            for name, values in metrics.items()
        ],
        "",
    ]
    preview = dict(explainability or {})
    top_logreg = list(preview.get("top_logreg_coefficients") or [])
    if top_logreg:
        lines.extend(["## Logistic Regression Coefficients", ""])
        lines.append(_render_markdown_table(pd.DataFrame(top_logreg)))
        lines.append("")
    top_lgb = list(preview.get("top_lgb_importance") or [])
    if top_lgb:
        lines.extend(["## LightGBM Feature Importance", ""])
        lines.append(_render_markdown_table(pd.DataFrame(top_lgb)))
        lines.append("")
    positive_factors = list(preview.get("top_positive_factors") or [])
    negative_factors = list(preview.get("top_negative_factors") or [])
    if positive_factors or negative_factors:
        lines.extend(["## Factor Direction Summary", ""])
        if positive_factors:
            lines.extend(["### Positive Factors", ""])
            lines.append(_render_markdown_table(pd.DataFrame(positive_factors)))
            lines.append("")
        if negative_factors:
            lines.extend(["### Negative Factors", ""])
            lines.append(_render_markdown_table(pd.DataFrame(negative_factors)))
            lines.append("")
    return "\n".join(lines)


def render_training_run_report(summary_payload: dict[str, object]) -> str:
    offset_rows = summary_payload.get("offset_summaries", [])
    lines = [
        "# Training Run Summary",
        "",
        f"- market: `{summary_payload.get('market')}`",
        f"- cycle: `{summary_payload.get('cycle')}`",
        f"- model_family: `{summary_payload.get('model_family')}`",
        f"- feature_set: `{summary_payload.get('feature_set')}`",
        f"- label_set: `{summary_payload.get('label_set')}`",
        f"- target: `{summary_payload.get('target')}`",
        f"- window: `{summary_payload.get('window')}`",
        f"- weight_variant_label: `{summary_payload.get('weight_variant_label', 'default')}`",
        "",
        "## Offset Metrics",
        "",
    ]
    if isinstance(offset_rows, list) and offset_rows:
        frame = pd.DataFrame(offset_rows)
        preferred = [
            "offset",
            "rows",
            "positive_rate",
            "dropped_features",
            "brier_lgb",
            "brier_lr",
            "brier_blend",
            "auc_lgb",
            "auc_lr",
            "auc_blend",
        ]
        columns = [column for column in preferred if column in frame.columns]
        lines.append(_render_markdown_table(frame.loc[:, columns]))
        lines.append("")
    else:
        lines.append("No offset summaries available.")
        lines.append("")
    return "\n".join(lines)


def _render_markdown_table(frame: pd.DataFrame) -> str:
    rendered = frame.astype("object").where(frame.notna(), "")
    try:
        return rendered.to_markdown(index=False)
    except ImportError:
        return _render_markdown_table_fallback(rendered)


def _render_markdown_table_fallback(frame: pd.DataFrame) -> str:
    columns = [str(column) for column in frame.columns.tolist()]
    if not columns:
        return ""
    header = "| " + " | ".join(_markdown_cell(column) for column in columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    rows = [
        "| " + " | ".join(_markdown_cell(value) for value in row) + " |"
        for row in frame.itertuples(index=False, name=None)
    ]
    return "\n".join([header, divider, *rows])


def _markdown_cell(value: object) -> str:
    text = "" if value is None else str(value)
    return text.replace("\n", "<br>").replace("|", "\\|")
