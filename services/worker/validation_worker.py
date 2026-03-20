"""
Validation worker.

Consumes: fhir-ingest
Produces: fhir-validated

For each job:
  1. Decode raw FHIR bytes from message
  2. Parse into ParsedFHIRPayload
  3. Look up NHANES reference ranges for each biomarker (age/sex stratified)
  4. Flag abnormal values with severity and z-score equivalent
  5. Compute data quality score
  6. Persist ValidationResult to Postgres
  7. Publish to fhir-validated topic
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
from shared.kafka.kafka_utils import (
    consume_loop,
    ensure_topics,
    get_consumer,
    get_producer,
    publish,
)
from shared.logging_config import configure_logging
from shared.models.models import (
    BiomarkerFlag,
    BiomarkerObservation,
    JobStatus,
    KafkaIngestMessage,
    KafkaValidatedMessage,
    ParsedFHIRPayload,
    Severity,
    ValidationResult,
)

configure_logging()
log = structlog.get_logger()

TOPIC_IN  = "fhir-ingest"
TOPIC_OUT = "fhir-validated"

# LOINC codes we consider "critical" — missing these degrades quality score heavily
CRITICAL_BIOMARKERS = {
    "2093-3",   # Total Cholesterol
    "2085-9",   # HDL
    "13457-7",  # LDL
    "3043-7",   # Triglycerides
    "2345-7",   # Glucose
    "718-7",    # Hemoglobin
}


# ---------------------------------------------------------------------------
# Reference range lookup
# ---------------------------------------------------------------------------

async def get_reference_ranges(
    session: AsyncSession,
    loinc_code: str,
    age: int | None,
    sex: str | None,
) -> ReferenceRangeRow | None:
    effective_sex = sex if sex in ("male", "female") else "all"
    effective_age = age if age is not None else 40  # fallback

    stmt = (
        select(ReferenceRangeRow)
        .where(
            ReferenceRangeRow.loinc_code == loinc_code,
            ReferenceRangeRow.sex == effective_sex,
            ReferenceRangeRow.age_min <= effective_age,
            ReferenceRangeRow.age_max >= effective_age,
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()

    # Fallback to "all" if sex-specific not found
    if row is None and effective_sex != "all":
        stmt2 = (
            select(ReferenceRangeRow)
            .where(
                ReferenceRangeRow.loinc_code == loinc_code,
                ReferenceRangeRow.sex == "all",
                ReferenceRangeRow.age_min <= effective_age,
                ReferenceRangeRow.age_max >= effective_age,
            )
            .limit(1)
        )
        result2 = await session.execute(stmt2)
        row = result2.scalar_one_or_none()

    return row


def classify_severity(value: float, ref: ReferenceRangeRow) -> tuple[Severity, float | None]:
    """
    Returns (severity, percentile_position).
    We use the NHANES distribution percentiles to give a richer assessment
    than a simple normal/abnormal binary.
    """
    percentiles = [
        (2.5,  ref.p2_5),
        (5,    ref.p5),
        (10,   ref.p10),
        (25,   ref.p25),
        (50,   ref.p50),
        (75,   ref.p75),
        (90,   ref.p90),
        (95,   ref.p95),
        (97.5, ref.p97_5),
    ]
    percentiles = [(p, v) for p, v in percentiles if v is not None]
    if not percentiles:
        return Severity.NORMAL, None

    # Interpolate where the value falls
    vals = [v for _, v in percentiles]
    pcts = [p for p, _ in percentiles]

    if value <= vals[0]:
        position = pcts[0]
    elif value >= vals[-1]:
        position = pcts[-1]
    else:
        for i in range(len(vals) - 1):
            if vals[i] <= value <= vals[i + 1]:
                frac = (value - vals[i]) / (vals[i + 1] - vals[i])
                position = pcts[i] + frac * (pcts[i + 1] - pcts[i])
                break
        else:
            position = 50.0

    if position <= 2.5 or position >= 97.5:
        return Severity.CRITICAL, position
    elif position <= 5 or position >= 95:
        return Severity.ABNORMAL, position
    elif position <= 10 or position >= 90:
        return Severity.BORDERLINE, position
    else:
        return Severity.NORMAL, position


def build_note(severity: Severity, position: float | None, obs: BiomarkerObservation) -> str:
    if position is None:
        return ""
    if position <= 2.5:
        return f"Critically low — below 2.5th percentile for age/sex cohort"
    elif position >= 97.5:
        return f"Critically high — above 97.5th percentile for age/sex cohort"
    elif position <= 5:
        return f"Very low — {position:.0f}th percentile"
    elif position >= 95:
        return f"Very high — {position:.0f}th percentile"
    elif position <= 10:
        return f"Low — {position:.0f}th percentile"
    elif position >= 90:
        return f"High — {position:.0f}th percentile"
    return f"{position:.0f}th percentile — within normal range"


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

async def handle_message_async(payload: dict) -> None:
    msg = KafkaIngestMessage(**payload)
    job_id = msg.job_id
    log.info("validation.start", job_id=job_id)

    raw = base64.b64decode(msg.raw_bytes_b64)

    async with AsyncSessionFactory() as session:
        # Mark job as validating
        await session.execute(
            update(JobRow)
            .where(JobRow.job_id == job_id)
            .values(status=JobStatus.VALIDATING, updated_at=datetime.utcnow())
        )
        await session.commit()

        try:
            parsed: ParsedFHIRPayload = parse_fhir(raw, msg.source_format)
            demo = parsed.demographics
            flags: list[BiomarkerFlag] = []
            found_loincs = {obs.loinc_code for obs in parsed.observations}
            missing_critical = list(CRITICAL_BIOMARKERS - found_loincs)

            for obs in parsed.observations:
                ref = await get_reference_ranges(session, obs.loinc_code, demo.age, demo.sex)
                if ref is None:
                    # No NHANES reference — flag as unknown severity
                    flags.append(BiomarkerFlag(
                        loinc_code=obs.loinc_code,
                        display_name=obs.display_name,
                        value=obs.value,
                        unit=obs.unit,
                        severity=Severity.NORMAL,
                        note="No population reference data available",
                    ))
                    continue

                severity, position = classify_severity(obs.value, ref)
                note = build_note(severity, position, obs)

                flags.append(BiomarkerFlag(
                    loinc_code=obs.loinc_code,
                    display_name=obs.display_name,
                    value=obs.value,
                    unit=obs.unit,
                    severity=severity,
                    z_score=position,
                    note=note,
                ))

            # Quality score: penalise missing critical biomarkers
            quality = max(0.0, 1.0 - (len(missing_critical) / len(CRITICAL_BIOMARKERS)) * 0.5)
            if len(parsed.observations) < 5:
                quality *= 0.7

            validation_result = ValidationResult(
                patient_id=demo.patient_id,
                flags=flags,
                missing_critical=missing_critical,
                data_quality_score=round(quality, 3),
            )

            await session.execute(
                update(JobRow)
                .where(JobRow.job_id == job_id)
                .values(
                    status=JobStatus.ANALYZING,
                    patient_id=demo.patient_id,
                    parsed_payload=parsed.model_dump(mode="json"),
                    validation_result=validation_result.model_dump(mode="json"),
                    updated_at=datetime.utcnow(),
                )
            )
            await session.commit()
            log.info("validation.complete", job_id=job_id, flags=len(flags), quality=quality)

        except Exception as exc:
            log.error("validation.failed", job_id=job_id, error=str(exc))
            await session.execute(
                update(JobRow)
                .where(JobRow.job_id == job_id)
                .values(status=JobStatus.FAILED, error=str(exc), updated_at=datetime.utcnow())
            )
            await session.commit()
            raise


_producer = None
_consumer = None


def handle_message(payload: dict) -> None:
    asyncio.get_event_loop().run_until_complete(handle_message_async(payload))
    # Publish downstream
    msg = KafkaValidatedMessage(
        job_id=payload["job_id"],
        patient_id=payload.get("patient_id", "unknown"),
    )
    publish(_producer, TOPIC_OUT, payload["job_id"], msg.model_dump())


def main() -> None:
    global _producer, _consumer
    ensure_topics([TOPIC_IN, TOPIC_OUT, TOPIC_OUT + ".dlq"])
    _producer = get_producer()
    _consumer = get_consumer("validation-group", [TOPIC_IN])
    log.info("validation_worker.started")
    consume_loop(_consumer, _producer, handle_message)


if __name__ == "__main__":
    asyncio.run(create_all_tables())
    main()
