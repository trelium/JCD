"""
Analysis worker.

Consumes: fhir-validated
Produces: fhir-analysed

For each job:
  1. Load ValidationResult from Postgres
  2. Compute cohort percentiles vs NHANES distribution
  3. Calculate composite risk score
  4. Build Plotly chart data (serialised to JSON)
  5. Generate plain-language key findings
  6. Persist AnalysisResult
  7. Publish to fhir-analysed
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime

import numpy as np
import plotly.graph_objects as go
import structlog
from plotly.utils import PlotlyJSONEncoder
import json
from sqlalchemy import select, update

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from shared.db.database import AsyncSessionFactory, JobRow, ReferenceRangeRow, create_all_tables
from shared.kafka.kafka_utils import (
    consume_loop, ensure_topics, get_consumer, get_producer, publish,
)
from shared.logging_config import configure_logging
from shared.models.models import (
    AnalysisResult, CohortPercentile, JobStatus,
    KafkaAnalysedMessage, KafkaValidatedMessage,
    RiskTier, Severity, ValidationResult,
)

configure_logging()
log = structlog.get_logger()

TOPIC_IN  = "fhir-validated"
TOPIC_OUT = "fhir-analysed"

# Risk weights per LOINC — how much each contributes to composite score
RISK_WEIGHTS: dict[str, float] = {
    "2093-3":  0.15,   # Total Cholesterol
    "13457-7": 0.20,   # LDL
    "2085-9":  0.15,   # HDL (inverted — high is good)
    "3043-7":  0.10,   # Triglycerides
    "2345-7":  0.15,   # Glucose
    "4548-4":  0.15,   # HbA1c
    "30522-7": 0.10,   # hsCRP
}

HDL_LOINC = "2085-9"    # higher HDL = lower risk — inverted in scoring


def percentile_to_risk_contribution(loinc: str, percentile: float) -> float:
    """
    Convert a percentile position (0-100) to a risk contribution (0-1).
    For HDL (protective), higher percentile = lower risk.
    For others, tails (both low and high) contribute to risk.
    """
    if loinc == HDL_LOINC:
        # HDL: low is bad — map low percentile to high risk
        return max(0.0, (50 - percentile) / 50) if percentile < 50 else 0.0

    # For most biomarkers, the upper tail is the primary risk
    if percentile >= 95:
        return 1.0
    elif percentile >= 90:
        return 0.75
    elif percentile >= 75:
        return 0.35
    elif percentile <= 5:
        return 0.8   # critically low also concerning
    elif percentile <= 10:
        return 0.4
    return 0.0


def compute_risk_score(flags, percentile_map: dict[str, float]) -> float:
    """Weighted composite risk score 0-100."""
    total_weight = 0.0
    weighted_risk = 0.0

    for flag in flags:
        loinc = flag["loinc_code"]
        weight = RISK_WEIGHTS.get(loinc, 0.05)
        percentile = percentile_map.get(loinc, 50.0)
        contribution = percentile_to_risk_contribution(loinc, percentile)
        weighted_risk += weight * contribution
        total_weight += weight

    if total_weight == 0:
        return 25.0  # default moderate-low

    normalised = (weighted_risk / total_weight) * 100
    # Severity boosts
    critical_count = sum(1 for f in flags if f.get("severity") == Severity.CRITICAL)
    normalised = min(100, normalised + critical_count * 8)
    return round(normalised, 1)


def score_to_tier(score: float) -> RiskTier:
    if score >= 70:
        return RiskTier.URGENT
    elif score >= 45:
        return RiskTier.HIGH
    elif score >= 20:
        return RiskTier.MODERATE
    return RiskTier.LOW


def build_findings(flags: list[dict], risk_tier: RiskTier, quality: float) -> list[str]:
    findings = []

    abnormal = [f for f in flags if f.get("severity") in (Severity.ABNORMAL, Severity.CRITICAL)]
    borderline = [f for f in flags if f.get("severity") == Severity.BORDERLINE]

    if risk_tier == RiskTier.URGENT:
        findings.append("⚠ Urgent: multiple critical biomarker values detected — immediate GP review required.")
    elif risk_tier == RiskTier.HIGH:
        findings.append("Elevated cardiovascular/metabolic risk profile — GP follow-up recommended.")
    elif risk_tier == RiskTier.MODERATE:
        findings.append("Moderate risk profile — some biomarkers outside optimal range.")
    else:
        findings.append("Low overall risk — biomarker profile largely within normal population range.")

    for f in abnormal[:4]:
        sev = f.get("severity", "")
        note = f.get("note", "")
        findings.append(f"{'🔴' if sev == Severity.CRITICAL else '🟠'} {f['display_name']}: {f['value']} {f.get('unit','')} — {note}")

    for f in borderline[:3]:
        findings.append(f"🟡 {f['display_name']}: {f['value']} {f.get('unit','')} — {f.get('note','borderline')}")

    if quality < 0.7:
        findings.append(f"⚠ Data quality score {quality:.0%} — some expected biomarkers missing from panel.")

    return findings


def build_chart(flags: list[dict], percentile_map: dict[str, float]) -> dict:
    """Build a Plotly horizontal bar chart of biomarker percentile positions."""
    names = []
    percentiles = []
    colors = []
    texts = []

    severity_color = {
        Severity.CRITICAL:   "#dc3545",
        Severity.ABNORMAL:   "#fd7e14",
        Severity.BORDERLINE: "#ffc107",
        Severity.NORMAL:     "#198754",
    }

    for f in flags:
        loinc = f["loinc_code"]
        if loinc not in percentile_map:
            continue
        p = percentile_map[loinc]
        names.append(f["display_name"])
        percentiles.append(p)
        colors.append(severity_color.get(f.get("severity", Severity.NORMAL), "#198754"))
        texts.append(f"{p:.0f}th pct")

    fig = go.Figure(go.Bar(
        x=percentiles,
        y=names,
        orientation="h",
        marker_color=colors,
        text=texts,
        textposition="outside",
        cliponaxis=False,
    ))
    fig.add_vline(x=5, line_dash="dot", line_color="#adb5bd", annotation_text="5th")
    fig.add_vline(x=95, line_dash="dot", line_color="#adb5bd", annotation_text="95th")
    fig.update_layout(
        title="Biomarker percentile positions vs NHANES population",
        xaxis_title="Population percentile",
        xaxis_range=[0, 105],
        height=max(300, len(names) * 28 + 80),
        margin=dict(l=20, r=60, t=50, b=40),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(size=11),
        showlegend=False,
    )
    return json.loads(json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder))


async def handle_message_async(payload: dict) -> None:
    msg = KafkaValidatedMessage(**payload)
    job_id = msg.job_id
    log.info("analysis.start", job_id=job_id)

    async with AsyncSessionFactory() as session:
        row = await session.get(JobRow, job_id)
        if not row or not row.validation_result:
            raise ValueError(f"No validation result found for job {job_id}")

        vr_data = row.validation_result
        flags = vr_data.get("flags", [])
        quality = vr_data.get("data_quality_score", 1.0)
        age = row.parsed_payload.get("demographics", {}).get("age") if row.parsed_payload else None
        sex = row.parsed_payload.get("demographics", {}).get("sex") if row.parsed_payload else None

        # Fetch percentiles for each biomarker
        percentile_map: dict[str, float] = {}
        cohort_percentiles: list[CohortPercentile] = []

        for flag in flags:
            loinc = flag["loinc_code"]
            if flag.get("z_score") is not None:
                p = float(flag["z_score"])
                percentile_map[loinc] = p
                cohort_percentiles.append(CohortPercentile(
                    loinc_code=loinc,
                    display_name=flag["display_name"],
                    patient_value=flag["value"],
                    percentile=p,
                ))

        risk_score = compute_risk_score(flags, percentile_map)
        risk_tier = score_to_tier(risk_score)
        findings = build_findings(flags, risk_tier, quality)
        chart_data = build_chart(flags, percentile_map)

        analysis_result = AnalysisResult(
            patient_id=msg.patient_id,
            risk_tier=risk_tier,
            risk_score=risk_score,
            cohort_percentiles=cohort_percentiles,
            key_findings=findings,
            chart_data=chart_data,
        )

        await session.execute(
            update(JobRow)
            .where(JobRow.job_id == job_id)
            .values(
                status=JobStatus.REPORTING,
                analysis_result=analysis_result.model_dump(mode="json"),
                updated_at=datetime.utcnow(),
            )
        )
        await session.commit()
        log.info("analysis.complete", job_id=job_id, risk_tier=risk_tier, score=risk_score)


_producer = None

def handle_message(payload: dict) -> None:
    asyncio.get_event_loop().run_until_complete(handle_message_async(payload))
    out = KafkaAnalysedMessage(job_id=payload["job_id"], patient_id=payload.get("patient_id",""))
    publish(_producer, TOPIC_OUT, payload["job_id"], out.model_dump())


def main() -> None:
    global _producer
    ensure_topics([TOPIC_IN, TOPIC_OUT, TOPIC_OUT + ".dlq"])
    _producer = get_producer()
    consumer = get_consumer("analysis-group", [TOPIC_IN])
    log.info("analysis_worker.started")
    consume_loop(consumer, _producer, handle_message)


if __name__ == "__main__":
    asyncio.run(create_all_tables())
    main()
