"""
Analysis worker — polls for jobs in status=analyzing.

Computes cohort percentiles, risk score, Plotly chart, key findings.
Advances job to status=reporting.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime

import structlog
from plotly.utils import PlotlyJSONEncoder
import plotly.graph_objects as go
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from shared.db.database import JobRow, create_all_tables
from shared.kafka.kafka_utils import consume_loop
from shared.logging_config import configure_logging
from shared.models.models import (
    AnalysisResult, CohortPercentile, JobStatus, RiskTier, Severity,
)

configure_logging()
log = structlog.get_logger()

RISK_WEIGHTS = {
    "2093-3": 0.15, "13457-7": 0.20, "2085-9": 0.15,
    "3043-7": 0.10, "2345-7":  0.15, "4548-4": 0.15, "30522-7": 0.10,
}
HDL_LOINC = "2085-9"

SEVERITY_COLOR = {
    Severity.CRITICAL:   "#dc3545",
    Severity.ABNORMAL:   "#fd7e14",
    Severity.BORDERLINE: "#ffc107",
    Severity.NORMAL:     "#198754",
}


def percentile_to_risk(loinc: str, pct: float) -> float:
    if loinc == HDL_LOINC:
        return max(0.0, (50 - pct) / 50) if pct < 50 else 0.0
    if pct >= 95: return 1.0
    if pct >= 90: return 0.75
    if pct >= 75: return 0.35
    if pct <= 5:  return 0.8
    if pct <= 10: return 0.4
    return 0.0


def compute_risk(flags, pct_map) -> float:
    total_w = weighted = 0.0
    for f in flags:
        loinc = f["loinc_code"]
        w = RISK_WEIGHTS.get(loinc, 0.05)
        pct = pct_map.get(loinc, 50.0)
        weighted += w * percentile_to_risk(loinc, pct)
        total_w += w
    if total_w == 0:
        return 25.0
    score = (weighted / total_w) * 100
    critical = sum(1 for f in flags if f.get("severity") == Severity.CRITICAL)
    return min(100, round(score + critical * 8, 1))


def score_to_tier(score: float) -> RiskTier:
    if score >= 70: return RiskTier.URGENT
    if score >= 45: return RiskTier.HIGH
    if score >= 20: return RiskTier.MODERATE
    return RiskTier.LOW


def build_findings(flags, tier, quality) -> list[str]:
    findings = []
    if tier == RiskTier.URGENT:
        findings.append("⚠ Urgent: multiple critical values — immediate GP review required.")
    elif tier == RiskTier.HIGH:
        findings.append("Elevated cardiovascular/metabolic risk — GP follow-up recommended.")
    elif tier == RiskTier.MODERATE:
        findings.append("Moderate risk — some biomarkers outside optimal range.")
    else:
        findings.append("Low overall risk — biomarker profile largely within normal range.")

    abnormal = [f for f in flags if f.get("severity") in (Severity.ABNORMAL, Severity.CRITICAL)]
    borderline = [f for f in flags if f.get("severity") == Severity.BORDERLINE]

    for f in abnormal[:4]:
        icon = "🔴" if f.get("severity") == Severity.CRITICAL else "🟠"
        findings.append(f"{icon} {f['display_name']}: {f['value']} {f.get('unit','')} — {f.get('note','')}")
    for f in borderline[:3]:
        findings.append(f"🟡 {f['display_name']}: {f['value']} {f.get('unit','')} — {f.get('note','')}")
    if quality < 0.7:
        findings.append(f"⚠ Data quality {quality:.0%} — some expected biomarkers missing.")
    return findings


def build_chart(flags, pct_map) -> dict:
    names, pcts, colors, texts = [], [], [], []
    for f in flags:
        loinc = f["loinc_code"]
        if loinc not in pct_map:
            continue
        p = pct_map[loinc]
        names.append(f["display_name"])
        pcts.append(p)
        colors.append(SEVERITY_COLOR.get(f.get("severity", Severity.NORMAL), "#198754"))
        texts.append(f"{p:.0f}th pct")

    fig = go.Figure(go.Bar(
        x=pcts, y=names, orientation="h",
        marker_color=colors, text=texts, textposition="outside", cliponaxis=False,
    ))
    fig.add_vline(x=5,  line_dash="dot", line_color="#adb5bd", annotation_text="5th")
    fig.add_vline(x=95, line_dash="dot", line_color="#adb5bd", annotation_text="95th")
    fig.update_layout(
        title="Biomarker percentile positions vs NHANES population",
        xaxis_title="Population percentile", xaxis_range=[0, 105],
        height=max(300, len(names) * 28 + 80),
        margin=dict(l=20, r=60, t=50, b=40),
        plot_bgcolor="white", paper_bgcolor="white",
        font=dict(size=11), showlegend=False,
    )
    return json.loads(json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder))


async def handle(row: JobRow, session: AsyncSession) -> None:
    job_id = row.job_id
    log.info("analysis.start", job_id=str(job_id))

    vr = row.validation_result or {}
    flags = vr.get("flags", [])
    quality = vr.get("data_quality_score", 1.0)

    pct_map: dict[str, float] = {}
    cohort_percentiles: list[CohortPercentile] = []
    for f in flags:
        if f.get("z_score") is not None:
            p = float(f["z_score"])
            pct_map[f["loinc_code"]] = p
            cohort_percentiles.append(CohortPercentile(
                loinc_code=f["loinc_code"],
                display_name=f["display_name"],
                patient_value=f["value"],
                percentile=p,
            ))

    risk_score = compute_risk(flags, pct_map)
    risk_tier  = score_to_tier(risk_score)
    findings   = build_findings(flags, risk_tier, quality)
    chart_data = build_chart(flags, pct_map)

    ar = AnalysisResult(
        patient_id=row.patient_id or "unknown",
        risk_tier=risk_tier,
        risk_score=risk_score,
        cohort_percentiles=cohort_percentiles,
        key_findings=findings,
        chart_data=chart_data,
    )

    await session.execute(
        update(JobRow).where(JobRow.job_id == job_id).values(
            status=JobStatus.REPORTING,
            analysis_result=ar.model_dump(mode="json"),
            updated_at=datetime.utcnow(),
        )
    )
    await session.commit()
    log.info("analysis.complete", job_id=str(job_id), tier=risk_tier, score=risk_score)


def main():
    asyncio.run(create_all_tables())
    consume_loop("analysis-worker", JobStatus.ANALYZING, handle)


if __name__ == "__main__":
    main()
