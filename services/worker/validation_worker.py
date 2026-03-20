"""
Validation worker — polls for jobs in status=pending.

Parses FHIR, validates biomarkers against NHANES reference ranges,
computes data quality score, advances job to status=validating (done).
"""
from __future__ import annotations

import asyncio
import base64
import os
import sys
from datetime import datetime

import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from services.worker.fhir_parser import parse_fhir
from shared.db.database import AsyncSessionFactory, JobRow, ReferenceRangeRow, create_all_tables
from shared.kafka.kafka_utils import consume_loop
from shared.logging_config import configure_logging
from shared.models.models import (
    BiomarkerFlag, JobStatus, ParsedFHIRPayload, Severity, ValidationResult,
)

configure_logging()
log = structlog.get_logger()

CRITICAL_BIOMARKERS = {
    "2093-3", "2085-9", "13457-7", "3043-7", "2345-7", "718-7",
}


async def get_reference_range(session, loinc_code, age, sex):
    from shared.db.database import ReferenceRangeRow
    effective_sex = sex if sex in ("male", "female") else "all"
    effective_age = age if age is not None else 40

    for s in (effective_sex, "all"):
        stmt = (
            select(ReferenceRangeRow)
            .where(
                ReferenceRangeRow.loinc_code == loinc_code,
                ReferenceRangeRow.sex == s,
                ReferenceRangeRow.age_min <= effective_age,
                ReferenceRangeRow.age_max >= effective_age,
            )
            .limit(1)
        )
        row = (await session.execute(stmt)).scalar_one_or_none()
        if row:
            return row
    return None


def classify(value, ref) -> tuple[Severity, float | None]:
    points = [(2.5, ref.p2_5),(5, ref.p5),(10, ref.p10),(25, ref.p25),
              (50, ref.p50),(75, ref.p75),(90, ref.p90),(95, ref.p95),(97.5, ref.p97_5)]
    points = [(p, v) for p, v in points if v is not None]
    if not points:
        return Severity.NORMAL, None

    vals = [v for _, v in points]
    pcts = [p for p, _ in points]

    if value <= vals[0]:
        pos = pcts[0]
    elif value >= vals[-1]:
        pos = pcts[-1]
    else:
        pos = 50.0
        for i in range(len(vals) - 1):
            if vals[i] <= value <= vals[i + 1]:
                frac = (value - vals[i]) / (vals[i + 1] - vals[i])
                pos = pcts[i] + frac * (pcts[i + 1] - pcts[i])
                break

    if pos <= 2.5 or pos >= 97.5:
        sev = Severity.CRITICAL
    elif pos <= 5 or pos >= 95:
        sev = Severity.ABNORMAL
    elif pos <= 10 or pos >= 90:
        sev = Severity.BORDERLINE
    else:
        sev = Severity.NORMAL

    return sev, pos


def make_note(sev, pos):
    if pos is None:
        return ""
    if pos <= 2.5:  return "Critically low — below 2.5th percentile"
    if pos >= 97.5: return "Critically high — above 97.5th percentile"
    if pos <= 5:    return f"Very low — {pos:.0f}th percentile"
    if pos >= 95:   return f"Very high — {pos:.0f}th percentile"
    if pos <= 10:   return f"Low — {pos:.0f}th percentile"
    if pos >= 90:   return f"High — {pos:.0f}th percentile"
    return f"{pos:.0f}th percentile — within normal range"


async def handle(row: JobRow, session: AsyncSession) -> None:
    job_id = row.job_id
    log.info("validation.start", job_id=str(job_id))

    raw_b64 = (row.parsed_payload or {}).get("raw_b64")
    if not raw_b64:
        raise ValueError("No raw FHIR bytes found on job row")
    raw = base64.b64decode(raw_b64)
    source_format = (row.parsed_payload or {}).get("source_format", "json")

    parsed: ParsedFHIRPayload = parse_fhir(raw, source_format)
    demo = parsed.demographics
    flags: list[BiomarkerFlag] = []
    found = {obs.loinc_code for obs in parsed.observations}
    missing_critical = list(CRITICAL_BIOMARKERS - found)

    for obs in parsed.observations:
        ref = await get_reference_range(session, obs.loinc_code, demo.age, demo.sex)
        if ref is None:
            flags.append(BiomarkerFlag(
                loinc_code=obs.loinc_code, display_name=obs.display_name,
                value=obs.value, unit=obs.unit, severity=Severity.NORMAL,
                note="No population reference data available",
            ))
            continue
        sev, pos = classify(obs.value, ref)
        flags.append(BiomarkerFlag(
            loinc_code=obs.loinc_code, display_name=obs.display_name,
            value=obs.value, unit=obs.unit, severity=sev,
            z_score=pos, note=make_note(sev, pos),
        ))

    quality = max(0.0, 1.0 - (len(missing_critical) / len(CRITICAL_BIOMARKERS)) * 0.5)
    if len(parsed.observations) < 5:
        quality *= 0.7

    vr = ValidationResult(
        patient_id=demo.patient_id,
        flags=flags,
        missing_critical=missing_critical,
        data_quality_score=round(quality, 3),
    )

    await session.execute(
        update(JobRow).where(JobRow.job_id == job_id).values(
            status=JobStatus.ANALYZING,
            patient_id=demo.patient_id,
            parsed_payload=parsed.model_dump(mode="json"),
            validation_result=vr.model_dump(mode="json"),
            updated_at=datetime.utcnow(),
        )
    )
    await session.commit()
    log.info("validation.complete", job_id=str(job_id), flags=len(flags))


def main():
    asyncio.run(create_all_tables())
    consume_loop("validation-worker", JobStatus.PENDING, handle)


if __name__ == "__main__":
    main()
