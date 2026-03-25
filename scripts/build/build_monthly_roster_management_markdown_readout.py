from __future__ import annotations

from pathlib import Path
import os
import pandas as pd


BASE_DIR = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

DEPLOY_FP = BASE_DIR / "data_processed/management/monthly_roster_deployment_recommendation.csv"
INTERP_FP = BASE_DIR / "data_processed/management/monthly_roster_management_interpretation.csv"
EXEC_FP = BASE_DIR / "data_processed/management/monthly_roster_executive_summary.csv"
PORT_FP = BASE_DIR / "data_processed/management/monthly_roster_portfolio_readout.csv"

OUT_DIR = BASE_DIR / "output/management"
OUT_FP = OUT_DIR / "monthly_roster_management_readout.md"


def safe_read_csv(fp: Path) -> pd.DataFrame:
    if not fp.exists():
        raise FileNotFoundError(f"Missing required file: {fp}")
    df = pd.read_csv(fp)
    if df.empty:
        raise ValueError(f"Input file is empty: {fp}")
    return df


def find_first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def fmt_int(x) -> str:
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "n/a"


def fmt_num(x, digits: int = 2) -> str:
    try:
        return f"{float(x):.{digits}f}"
    except Exception:
        return "n/a"


def value_counts_block(series: pd.Series) -> str:
    vc = series.fillna("unknown").astype(str).value_counts(dropna=False)
    return "\n".join(f"- {k}: {v:,}" for k, v in vc.items())


def top_rows_block(
    df: pd.DataFrame,
    sort_col: str | None,
    cols: list[str],
    top_n: int = 10,
    ascending: bool = False,
) -> str:
    use_cols = [c for c in cols if c in df.columns]
    if not use_cols:
        return "_No compatible columns available._"

    work = df.copy()
    if sort_col and sort_col in work.columns:
        work[sort_col] = pd.to_numeric(work[sort_col], errors="coerce")
        work = work.sort_values(sort_col, ascending=ascending, na_position="last")

    work = work[use_cols].head(top_n).fillna("n/a")

    header = "| " + " | ".join(use_cols) + " |"
    sep = "| " + " | ".join(["---"] * len(use_cols)) + " |"
    rows = [
        "| " + " | ".join(str(row[c]) for c in use_cols) + " |"
        for _, row in work.iterrows()
    ]
    return "\n".join([header, sep] + rows)


def choose_month_label(df_list: list[pd.DataFrame]) -> str:
    candidates = [
        "month_id",
        "month",
        "month_label",
        "period",
        "period_month",
        "year_month",
        "report_month",
        "month_start",
        "period_start",
        "snapshot_month",
        "reporting_month",
        "as_of_month",
    ]
    seen = []
    for df in df_list:
        c = find_first_existing(df, candidates)
        if c:
            vals = df[c].dropna().astype(str).unique().tolist()
            seen.extend(vals)
    return sorted(seen)[-1] if seen else "latest available month"


def build_document(
    deploy: pd.DataFrame,
    interp: pd.DataFrame,
    exe: pd.DataFrame,
    port: pd.DataFrame,
) -> str:
    month_label = choose_month_label([deploy, interp, exe, port])

    outlet_col = find_first_existing(interp, ["outlet_key", "outlet_name", "site_name", "outlet"])
    month_col = find_first_existing(interp, ["month_id", "month", "year_month"])

    priority_col = find_first_existing(interp, ["roster_decision_priority_band", "priority_band"])
    focus_col = find_first_existing(interp, ["recommended_management_focus", "management_focus"])
    regime_col = find_first_existing(interp, ["external_context_regime", "market_context_regime"])
    score_col = find_first_existing(
        interp,
        ["roster_decision_priority_score_0_100", "deployment_adjustment_score_0_100", "management_attention_score_0_100"],
    )

    headline_col = find_first_existing(
        port,
        ["portfolio_staffing_headline", "portfolio_headline", "portfolio_readout_headline", "headline"],
    )
    takeaway_col = find_first_existing(
        port,
        ["portfolio_staffing_takeaway", "portfolio_takeaway"],
    )

    executive_headline_col = find_first_existing(
        exe,
        ["executive_summary_headline", "executive_headline"],
    )
    executive_detail_col = find_first_existing(
        exe,
        ["executive_summary_detail", "executive_summary", "executive_readout"],
    )

    boundary_col = find_first_existing(
        interp,
        ["evidence_boundary_note", "boundary_note", "method_boundary_note", "boundary_guardrail_note"],
    )

    total_rows = len(interp)
    total_outlets = interp[outlet_col].nunique() if outlet_col else None

    priority_block = value_counts_block(interp[priority_col]) if priority_col else "_Priority band column not found._"
    focus_block = value_counts_block(interp[focus_col]) if focus_col else "_Management focus column not found._"
    regime_block = value_counts_block(interp[regime_col]) if regime_col else "_External context regime column not found._"

    top_priority_table = top_rows_block(
        interp,
        sort_col=score_col,
        cols=[c for c in [outlet_col, month_col, priority_col, focus_col, regime_col, score_col] if c is not None],
        top_n=10,
        ascending=False,
    )

    headline_block = "_No portfolio headline column found._"
    if headline_col and headline_col in port.columns:
        vals = port[headline_col].dropna().astype(str).value_counts().head(8)
        if len(vals):
            headline_block = "\n".join(f"- {k} ({v:,})" for k, v in vals.items())

    executive_excerpt = "_No executive summary text column found._"
    if executive_detail_col and executive_detail_col in exe.columns:
        snippets = exe[executive_detail_col].dropna().astype(str).head(5).tolist()
        if snippets:
            executive_excerpt = "\n".join(f"- {s}" for s in snippets)
    elif executive_headline_col and executive_headline_col in exe.columns:
        snippets = exe[executive_headline_col].dropna().astype(str).head(5).tolist()
        if snippets:
            executive_excerpt = "\n".join(f"- {s}" for s in snippets)

    boundary_note = (
        str(interp[boundary_col].dropna().iloc[0]).strip()
        if boundary_col and interp[boundary_col].dropna().shape[0] > 0
        else (
            "This readout uses external market proxies only as contextual regime signals. "
            "Actionable staffing interpretation remains anchored to internal operating proxies. "
            "The layer does not infer hourly demand truth, daypart truth, or direct roster-by-hour needs "
            "from airport, entry-exit, or transport context."
        )
    )

    latest_takeaways = "_No portfolio takeaway column found._"
    if takeaway_col and takeaway_col in port.columns:
        tmp = port.copy()
        if month_col and month_col in tmp.columns:
            latest_month = sorted(tmp[month_col].dropna().astype(str).unique())[-1]
            tmp = tmp[tmp[month_col].astype(str) == latest_month]
        use_cols = [c for c in [outlet_col, takeaway_col] if c in tmp.columns]
        if use_cols:
            latest_takeaways = "\n".join(
                f"- {row[use_cols[0]]}: {row[use_cols[1]]}"
                for _, row in tmp[use_cols].head(5).fillna("n/a").iterrows()
            )

    lines = [
        "# Monthly Roster Management Readout",
        "",
        f"**Reporting month:** {month_label}",
        "",
        "## Purpose",
        "",
        "This management readout translates the validated monthly roster layers into a recruiter-friendly and executive-friendly narrative. It is designed to show how internal operating signals can be summarized into defensible management posture, without over-claiming external proxy data as direct hourly spa demand.",
        "",
        "## Management framing",
        "",
        "The readout is anchored to internal operating proxies for deployment and staffing interpretation. External market proxies are retained only as context for pressure regime and operating backdrop. This means the output supports management judgement, not pseudo-precision.",
        "",
        "## Portfolio snapshot",
        "",
        f"- Total monthly rows reviewed: {fmt_int(total_rows)}",
    ]

    if total_outlets is not None:
        lines.append(f"- Unique outlets represented: {fmt_int(total_outlets)}")

    lines += [
        "- Source layers combined: 4",
        "- Output orientation: monthly management narrative",
        "- Boundary posture: no hourly/daypart truth claims",
        "",
        "## Priority band distribution",
        "",
        priority_block,
        "",
        "## Recommended management focus distribution",
        "",
        focus_block,
        "",
        "## External context regime distribution",
        "",
        regime_block,
        "",
        "## Portfolio management headlines",
        "",
        headline_block,
        "",
        "## Highest-attention monthly cases",
        "",
        "The table below is intended to surface where management attention should go first. It should be interpreted as a monthly prioritization lens, not as proof of hourly demand structure.",
        "",
        top_priority_table,
        "",
        "## Executive narrative excerpt",
        "",
        executive_excerpt,
        "",
        "## Latest-month portfolio takeaways",
        "",
        latest_takeaways,
        "",
        "## Interpretation boundary",
        "",
        boundary_note,
        "",
        "## How this layer should be used",
        "",
        "- Use this readout to explain staffing posture and deployment attention at monthly level.",
        "- Use internal operating proxies as the anchor for actionability.",
        "- Use external context only to frame whether pressure is supportive, soft, or neutral.",
        "- Do not use this layer to claim direct daypart demand truth or hourly staffing requirements.",
        "- Do not convert transport or passenger context into pseudo-granular roster prescriptions.",
        "",
        "## Recruiter-facing value",
        "",
        "This layer demonstrates management discipline rather than dashboard ornament. It shows that staffing interpretation, prioritization, and portfolio commentary can be made readable for leadership audiences while staying methodologically defensible.",
        "",
        "## File lineage",
        "",
        f"- `{DEPLOY_FP.relative_to(BASE_DIR)}`",
        f"- `{INTERP_FP.relative_to(BASE_DIR)}`",
        f"- `{EXEC_FP.relative_to(BASE_DIR)}`",
        f"- `{PORT_FP.relative_to(BASE_DIR)}`",
        "",
        "## Output note",
        "",
        "This markdown file is a communication layer built from validated monthly management datasets. It is not presented as a live production scheduling system and should not be read as a direct hourly staffing engine.",
        "",
    ]

    return "\n".join(lines)


def main() -> None:
    deploy = safe_read_csv(DEPLOY_FP)
    interp = safe_read_csv(INTERP_FP)
    exe = safe_read_csv(EXEC_FP)
    port = safe_read_csv(PORT_FP)

    os.makedirs(OUT_DIR, exist_ok=True)

    doc = build_document(deploy, interp, exe, port)
    OUT_FP.write_text(doc, encoding="utf-8")

    print(f"[OK] saved: {OUT_FP}")


if __name__ == "__main__":
    main()
