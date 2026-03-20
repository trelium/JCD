from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class JobStatus(str, Enum):
    PENDING   = "pending"
    VALIDATING = "validating"
    ANALYZING  = "analyzing"
    REPORTING  = "reporting"
    COMPLETE   = "complete"
    FAILED     = "failed"


class Severity(str, Enum):
    NORMAL   = "normal"
    BORDERLINE = "borderline"
    ABNORMAL = "abnormal"
    CRITICAL = "critical"


# ---------------------------------------------------------------------------
# FHIR-derived biomarker observation (internal representation)
# ---------------------------------------------------------------------------

class BiomarkerObservation(BaseModel):
    loinc_code: str
    display_name: str
    value: float
    unit: str
    reference_low: float | None = None
    reference_high: float | None = None
    effective_date: datetime | None = None

    @field_validator("value")
    @classmethod
    def value_must_be_finite(cls, v: float) -> float:
        import math
        if not math.isfinite(v):
            raise ValueError("Biomarker value must be a finite number")
        return v


class PatientDemographics(BaseModel):
    patient_id: str
    age: int | None = None
    sex: str | None = None          # "male" | "female" | "unknown"
    ethnicity: str | None = None


class ParsedFHIRPayload(BaseModel):
    """Internal representation after parsing raw FHIR JSON/XML."""
    demographics: PatientDemographics
    observations: list[BiomarkerObservation]
    raw_resource_count: int
    source_format: str              # "json" | "xml"


# ---------------------------------------------------------------------------
# Validation stage output
# ---------------------------------------------------------------------------

class BiomarkerFlag(BaseModel):
    loinc_code: str
    display_name: str
    value: float
    unit: str
    severity: Severity
    z_score: float | None = None    # relative to NHANES population
    note: str | None = None


class ValidationResult(BaseModel):
    patient_id: str
    flags: list[BiomarkerFlag]
    missing_critical: list[str]     # LOINC codes expected but absent
    data_quality_score: float       # 0–1
    validated_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Analysis stage output
# ---------------------------------------------------------------------------

class RiskTier(str, Enum):
    LOW      = "low"
    MODERATE = "moderate"
    HIGH     = "high"
    URGENT   = "urgent"


class CohortPercentile(BaseModel):
    loinc_code: str
    display_name: str
    patient_value: float
    percentile: float               # vs NHANES age/sex cohort


class AnalysisResult(BaseModel):
    patient_id: str
    risk_tier: RiskTier
    risk_score: float               # 0–100
    cohort_percentiles: list[CohortPercentile]
    key_findings: list[str]         # plain-language bullet strings
    chart_data: dict[str, Any]      # serialised Plotly figure JSON
    analysed_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Report stage output
# ---------------------------------------------------------------------------

class ReportResult(BaseModel):
    patient_id: str
    html_report: str
    narrative_summary: str          # LLM-generated paragraph
    generated_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Job envelope (persisted in Postgres, published to Kafka)
# ---------------------------------------------------------------------------

class Job(BaseModel):
    job_id: UUID = Field(default_factory=uuid4)
    input_hash: str                 # SHA-256 of raw upload bytes
    status: JobStatus = JobStatus.PENDING
    patient_id: str | None = None
    error: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    parsed_payload: ParsedFHIRPayload | None = None
    validation_result: ValidationResult | None = None
    analysis_result: AnalysisResult | None = None
    report_result: ReportResult | None = None


# ---------------------------------------------------------------------------
# Kafka message envelopes
# ---------------------------------------------------------------------------

class KafkaIngestMessage(BaseModel):
    job_id: str
    input_hash: str
    raw_bytes_b64: str              # base64-encoded upload content
    source_format: str


class KafkaValidatedMessage(BaseModel):
    job_id: str
    patient_id: str


class KafkaAnalysedMessage(BaseModel):
    job_id: str
    patient_id: str
