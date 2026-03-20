"""
Report worker.

Consumes: fhir-analysed
Produces: (nothing — terminal stage)

For each job:
  1. Load AnalysisResult + ValidationResult from Postgres
  2. Call Claude API to generate GP-ready narrative
  3. Render full HTML report with embedded Plotly chart
  4. Persist ReportResult, mark job COMPLETE
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime

import httpx
import structlog
from sqlalchemy import update

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from shared.db.database import AsyncSessionFactory, JobRow, create_all_tables
from shared.kafka.kafka_utils import consume_loop, ensure_topics, get_consumer, get_producer
from shared.logging_config import configure_logging
from shared.models.models import (
    JobStatus, KafkaAnalysedMessage, ReportResult, RiskTier,
)

configure_logging()
log = structlog.get_logger()

TOPIC_IN      = "fhir-analysed"
ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL  = "claude-sonnet-4-20250514"


# ---------------------------------------------------------------------------
# Claude API call
# ---------------------------------------------------------------------------

async def generate_narrative(
    patient_id: str,
    age: int | None,
    sex: str | None,
    risk_tier: str,
    risk_score: float,
    findings: list[str],
    flags: list[dict],
    quality: float,
) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return _fallback_narrative(risk_tier, findings)

    abnormal_flags = [
        f"{f['display_name']}: {f['value']} {f.get('unit','')} ({f.get('note','')})"
        for f in flags
        if f.get("severity") in ("abnormal", "critical")
    ]

    prompt = f"""You are a clinical data analyst preparing a summary for a GP (general practitioner) 
to review before a patient consultation.

Patient: {patient_id}
Age: {age if age else 'unknown'}  Sex: {sex if sex else 'unknown'}
Overall risk tier: {risk_tier.upper()}  (composite score: {risk_score:.0f}/100)
Data quality: {quality:.0%}

Abnormal/critical findings:
{chr(10).join(abnormal_flags) if abnormal_flags else 'None'}

Key analysis findings:
{chr(10).join(findings)}

Write a concise clinical narrative (3-4 paragraphs) for the GP that:
1. Summarises the overall health picture in plain medical language
2. Highlights the most clinically significant findings and their implications
3. Suggests what areas the GP should probe in the consultation
4. Notes any data quality concerns if relevant

Be factual and clinical. Do not diagnose. Do not prescribe. Flag uncertainty where data is limited.
"""

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            ANTHROPIC_API,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": CLAUDE_MODEL,
                "max_tokens": 800,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        r.raise_for_status()
        data = r.json()
        return data["content"][0]["text"]


def _fallback_narrative(risk_tier: str, findings: list[str]) -> str:
    return (
        f"Risk tier: {risk_tier.upper()}. "
        "Automated narrative unavailable (ANTHROPIC_API_KEY not set). "
        "Please review the flagged biomarkers and percentile chart below.\n\n"
        + "\n".join(findings)
    )


# ---------------------------------------------------------------------------
# HTML report renderer
# ---------------------------------------------------------------------------

TIER_COLORS = {
    "low":      ("#d1e7dd", "#0a3622"),
    "moderate": ("#fff3cd", "#664d03"),
    "high":     ("#ffe5d0", "#7d2c00"),
    "urgent":   ("#f8d7da", "#58151c"),
}

TIER_LABELS = {
    "low": "Low Risk",
    "moderate": "Moderate Risk",
    "high": "High Risk",
    "urgent": "⚠ Urgent — Immediate Review",
}

SEVERITY_BADGE = {
    "normal":    ('<span style="background:#d1e7dd;color:#0a3622;padding:2px 8px;border-radius:12px;font-size:11px">Normal</span>'),
    "borderline":('<span style="background:#fff3cd;color:#664d03;padding:2px 8px;border-radius:12px;font-size:11px">Borderline</span>'),
    "abnormal":  ('<span style="background:#ffe5d0;color:#7d2c00;padding:2px 8px;border-radius:12px;font-size:11px">Abnormal</span>'),
    "critical":  ('<span style="background:#f8d7da;color:#58151c;padding:2px 8px;border-radius:12px;font-size:11px">Critical</span>'),
}


def render_html_report(
    patient_id: str,
    age: int | None,
    sex: str | None,
    risk_tier: str,
    risk_score: float,
    narrative: str,
    findings: list[str],
    flags: list[dict],
    quality: float,
    chart_data: dict,
    generated_at: datetime,
) -> str:
    tier_bg, tier_fg = TIER_COLORS.get(risk_tier, ("#f8f9fa", "#212529"))
    tier_label = TIER_LABELS.get(risk_tier, risk_tier.upper())

    flags_rows = ""
    for f in sorted(flags, key=lambda x: {"critical":0,"abnormal":1,"borderline":2,"normal":3}.get(x.get("severity","normal"),3)):
        badge = SEVERITY_BADGE.get(f.get("severity","normal"), "")
        flags_rows += f"""
        <tr>
          <td style="padding:8px 12px">{f['display_name']}</td>
          <td style="padding:8px 12px;text-align:right">{f['value']}</td>
          <td style="padding:8px 12px">{f.get('unit','')}</td>
          <td style="padding:8px 12px">{badge}</td>
          <td style="padding:8px 12px;color:#6c757d;font-size:12px">{f.get('note','')}</td>
        </tr>"""

    findings_html = "".join(f"<li style='margin-bottom:6px'>{fn}</li>" for fn in findings)
    chart_json = json.dumps(chart_data)

    narrative_html = "".join(
        f"<p style='margin-bottom:12px'>{para}</p>"
        for para in narrative.split("\n\n") if para.strip()
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Biomarker Report — {patient_id}</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: system-ui, -apple-system, sans-serif; background: #f8f9fa; color: #212529; line-height: 1.6; }}
  .container {{ max-width: 900px; margin: 2rem auto; padding: 0 1rem; }}
  .header {{ background: white; border-radius: 12px; padding: 2rem; margin-bottom: 1.5rem; box-shadow: 0 1px 4px rgba(0,0,0,0.06); display: flex; justify-content: space-between; align-items: flex-start; flex-wrap: wrap; gap: 1rem; }}
  .header h1 {{ font-size: 1.4rem; font-weight: 600; margin-bottom: 0.25rem; }}
  .header .meta {{ color: #6c757d; font-size: 0.875rem; }}
  .risk-badge {{ padding: 0.5rem 1.25rem; border-radius: 8px; font-weight: 600; font-size: 1rem; background: {tier_bg}; color: {tier_fg}; }}
  .card {{ background: white; border-radius: 12px; padding: 1.75rem; margin-bottom: 1.5rem; box-shadow: 0 1px 4px rgba(0,0,0,0.06); }}
  .card h2 {{ font-size: 1rem; font-weight: 600; margin-bottom: 1rem; padding-bottom: 0.5rem; border-bottom: 1px solid #e9ecef; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.875rem; }}
  thead th {{ text-align: left; padding: 8px 12px; font-weight: 500; color: #6c757d; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; border-bottom: 1px solid #e9ecef; }}
  tbody tr:hover {{ background: #f8f9fa; }}
  tbody tr {{ border-bottom: 1px solid #f1f3f5; }}
  .quality-bar {{ height: 8px; background: #e9ecef; border-radius: 4px; overflow: hidden; margin-top: 4px; }}
  .quality-fill {{ height: 100%; background: {'#198754' if quality > 0.8 else '#ffc107' if quality > 0.5 else '#dc3545'}; width: {quality*100:.0f}%; border-radius: 4px; }}
  .findings-list {{ list-style: none; padding: 0; }}
  .footer {{ text-align: center; color: #adb5bd; font-size: 0.8rem; padding: 1rem 0 2rem; }}
  @media print {{ body {{ background: white; }} .container {{ max-width: 100%; }} }}
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <div>
      <h1>Blood Panel Report</h1>
      <div class="meta">
        Patient: <strong>{patient_id}</strong> &nbsp;·&nbsp;
        Age: {age or '—'} &nbsp;·&nbsp;
        Sex: {sex or '—'} &nbsp;·&nbsp;
        Generated: {generated_at.strftime('%Y-%m-%d %H:%M UTC')}
      </div>
    </div>
    <div>
      <div class="risk-badge">{tier_label}</div>
      <div style="text-align:right;font-size:12px;color:#6c757d;margin-top:4px">Score: {risk_score:.0f}/100</div>
    </div>
  </div>

  <div class="card">
    <h2>Clinical narrative</h2>
    <div style="font-size:0.925rem;color:#333">{narrative_html}</div>
  </div>

  <div class="card">
    <h2>Key findings</h2>
    <ul class="findings-list">{findings_html}</ul>
  </div>

  <div class="card">
    <h2>Biomarker results</h2>
    <table>
      <thead><tr>
        <th>Biomarker</th><th>Value</th><th>Unit</th><th>Status</th><th>Note</th>
      </tr></thead>
      <tbody>{flags_rows}</tbody>
    </table>
  </div>

  <div class="card">
    <h2>Population percentile chart</h2>
    <div id="chart" style="width:100%"></div>
  </div>

  <div class="card" style="padding:1.25rem 1.75rem">
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:0.5rem">
      <div>
        <div style="font-size:0.8rem;color:#6c757d;font-weight:500;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:2px">Data quality</div>
        <div style="font-size:0.875rem">{quality:.0%}</div>
        <div class="quality-bar"><div class="quality-fill"></div></div>
      </div>
      <div style="font-size:0.8rem;color:#adb5bd;max-width:360px;text-align:right">
        Reference ranges derived from NHANES 2017–2018 population data, stratified by age and sex.
        This report is for GP review only and does not constitute a diagnosis.
      </div>
    </div>
  </div>

  <div class="footer">Biomarker Pipeline · Ahead Health Demo · {generated_at.strftime('%Y')}</div>
</div>

<script>
Plotly.newPlot('chart', {chart_json}.data, {chart_json}.layout, {{responsive: true, displayModeBar: false}});
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

async def handle_message_async(payload: dict) -> None:
    msg = KafkaAnalysedMessage(**payload)
    job_id = msg.job_id
    log.info("report.start", job_id=job_id)

    async with AsyncSessionFactory() as session:
        row = await session.get(JobRow, job_id)
        if not row or not row.analysis_result:
            raise ValueError(f"No analysis result for job {job_id}")

        ar = row.analysis_result
        vr = row.validation_result or {}
        pd_data = row.parsed_payload or {}

        age = pd_data.get("demographics", {}).get("age")
        sex = pd_data.get("demographics", {}).get("sex")
        patient_id = row.patient_id or "unknown"
        flags = vr.get("flags", [])
        quality = vr.get("data_quality_score", 1.0)
        risk_tier = ar.get("risk_tier", "low")
        risk_score = ar.get("risk_score", 0.0)
        findings = ar.get("key_findings", [])
        chart_data = ar.get("chart_data", {})

        narrative = await generate_narrative(
            patient_id, age, sex, risk_tier, risk_score, findings, flags, quality
        )

        now = datetime.utcnow()
        html_report = render_html_report(
            patient_id, age, sex, risk_tier, risk_score,
            narrative, findings, flags, quality, chart_data, now
        )

        report_result = ReportResult(
            patient_id=patient_id,
            html_report=html_report,
            narrative_summary=narrative,
            generated_at=now,
        )

        await session.execute(
            update(JobRow)
            .where(JobRow.job_id == job_id)
            .values(
                status=JobStatus.COMPLETE,
                report_result=report_result.model_dump(mode="json"),
                updated_at=now,
            )
        )
        await session.commit()
        log.info("report.complete", job_id=job_id)


_producer = None

def handle_message(payload: dict) -> None:
    asyncio.get_event_loop().run_until_complete(handle_message_async(payload))


def main() -> None:
    ensure_topics([TOPIC_IN])
    _producer = get_producer()
    consumer = get_consumer("report-group", [TOPIC_IN])
    log.info("report_worker.started")
    consume_loop(consumer, _producer, handle_message)


if __name__ == "__main__":
    asyncio.run(create_all_tables())
    main()
