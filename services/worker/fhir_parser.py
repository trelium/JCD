"""
Parse a raw FHIR Bundle (JSON or XML) into our internal ParsedFHIRPayload.

Supports:
  - FHIR R4 Bundle containing Patient + Observation resources
  - LOINC-coded observations (blood panel results)
  - Both JSON and XML serialisations

We use fhir.resources for JSON parsing (it validates the schema) and
xml.etree for XML (avoiding a heavy lxml dependency).
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime

import structlog
from fhir.resources.bundle import Bundle
from fhir.resources.observation import Observation
from fhir.resources.patient import Patient

from shared.models.models import (
    BiomarkerObservation,
    ParsedFHIRPayload,
    PatientDemographics,
)

log = structlog.get_logger()

FHIR_NS = "http://hl7.org/fhir"


def parse_fhir(raw: bytes, source_format: str) -> ParsedFHIRPayload:
    if source_format == "xml":
        return _parse_xml(raw)
    return _parse_json(raw)


# ---------------------------------------------------------------------------
# JSON parser
# ---------------------------------------------------------------------------

def _parse_json(raw: bytes) -> ParsedFHIRPayload:
    import json

    data = json.loads(raw)
    bundle = Bundle.parse_obj(data)

    demographics = PatientDemographics(patient_id="unknown")
    observations: list[BiomarkerObservation] = []
    resource_count = len(bundle.entry or [])

    for entry in bundle.entry or []:
        resource = entry.resource
        if resource is None:
            continue

        if resource.resource_type == "Patient":
            demographics = _extract_demographics_json(resource)
        elif resource.resource_type == "Observation":
            obs = _extract_observation_json(resource)
            if obs:
                observations.append(obs)

    return ParsedFHIRPayload(
        demographics=demographics,
        observations=observations,
        raw_resource_count=resource_count,
        source_format="json",
    )


def _extract_demographics_json(patient: Patient) -> PatientDemographics:
    patient_id = patient.id or "unknown"
    age: int | None = None
    sex: str | None = None

    if patient.birthDate:
        try:
            bd = datetime.strptime(str(patient.birthDate), "%Y-%m-%d")
            age = (datetime.utcnow() - bd).days // 365
        except Exception:
            pass

    if patient.gender:
        sex = "male" if patient.gender == "male" else "female"

    return PatientDemographics(patient_id=patient_id, age=age, sex=sex)


def _extract_observation_json(obs: Observation) -> BiomarkerObservation | None:
    # Must have a LOINC code
    loinc_code: str | None = None
    display_name: str = "Unknown"

    if obs.code and obs.code.coding:
        for coding in obs.code.coding:
            if coding.system and "loinc" in coding.system.lower():
                loinc_code = coding.code
                display_name = coding.display or display_name
                break

    if not loinc_code:
        return None

    # Numeric value
    value: float | None = None
    unit: str = ""
    ref_low: float | None = None
    ref_high: float | None = None

    if obs.valueQuantity:
        value = obs.valueQuantity.value
        unit = obs.valueQuantity.unit or ""

    if value is None:
        return None

    if obs.referenceRange:
        rr = obs.referenceRange[0]
        if rr.low and rr.low.value is not None:
            ref_low = float(rr.low.value)
        if rr.high and rr.high.value is not None:
            ref_high = float(rr.high.value)

    effective: datetime | None = None
    if obs.effectiveDateTime:
        try:
            effective = datetime.fromisoformat(str(obs.effectiveDateTime).replace("Z", "+00:00"))
        except Exception:
            pass

    return BiomarkerObservation(
        loinc_code=loinc_code,
        display_name=display_name,
        value=float(value),
        unit=unit,
        reference_low=ref_low,
        reference_high=ref_high,
        effective_date=effective,
    )


# ---------------------------------------------------------------------------
# XML parser
# ---------------------------------------------------------------------------

def _parse_xml(raw: bytes) -> ParsedFHIRPayload:
    root = ET.fromstring(raw)

    def ns(tag: str) -> str:
        return f"{{{FHIR_NS}}}{tag}"

    def val(el: ET.Element | None) -> str | None:
        if el is None:
            return None
        v = el.get("value")
        return v

    demographics = PatientDemographics(patient_id="unknown")
    observations: list[BiomarkerObservation] = []
    resource_count = 0

    for entry in root.findall(f".//{ns('entry')}"):
        resource_count += 1
        patient_el = entry.find(f".//{ns('Patient')}")
        obs_el = entry.find(f".//{ns('Observation')}")

        if patient_el is not None:
            demographics = _extract_demographics_xml(patient_el, ns, val)
        elif obs_el is not None:
            obs = _extract_observation_xml(obs_el, ns, val)
            if obs:
                observations.append(obs)

    return ParsedFHIRPayload(
        demographics=demographics,
        observations=observations,
        raw_resource_count=resource_count,
        source_format="xml",
    )


def _extract_demographics_xml(el, ns, val) -> PatientDemographics:
    patient_id = val(el.find(f"{ns('id')}")) or "unknown"
    age: int | None = None
    sex: str | None = None

    bd_el = el.find(f"{ns('birthDate')}")
    if bd_el is not None:
        bd_str = val(bd_el)
        if bd_str:
            try:
                bd = datetime.strptime(bd_str, "%Y-%m-%d")
                age = (datetime.utcnow() - bd).days // 365
            except Exception:
                pass

    gender_el = el.find(f"{ns('gender')}")
    if gender_el is not None:
        g = val(gender_el) or ""
        sex = "male" if g == "male" else "female"

    return PatientDemographics(patient_id=patient_id, age=age, sex=sex)


def _extract_observation_xml(el, ns, val) -> BiomarkerObservation | None:
    loinc_code: str | None = None
    display_name = "Unknown"

    for coding in el.findall(f".//{ns('coding')}"):
        system = val(coding.find(ns("system"))) or ""
        if "loinc" in system.lower():
            loinc_code = val(coding.find(ns("code")))
            display_name = val(coding.find(ns("display"))) or display_name
            break

    if not loinc_code:
        return None

    vq = el.find(f".//{ns('valueQuantity')}")
    if vq is None:
        return None

    value_str = val(vq.find(ns("value")))
    if value_str is None:
        return None

    try:
        value = float(value_str)
    except ValueError:
        return None

    unit = val(vq.find(ns("unit"))) or ""

    ref_low: float | None = None
    ref_high: float | None = None
    for rr in el.findall(f".//{ns('referenceRange')}"):
        low_el = rr.find(f".//{ns('low')}/{ns('value')}")
        high_el = rr.find(f".//{ns('high')}/{ns('value')}")
        if low_el is not None:
            try: ref_low = float(val(low_el) or "")
            except Exception: pass
        if high_el is not None:
            try: ref_high = float(val(high_el) or "")
            except Exception: pass

    return BiomarkerObservation(
        loinc_code=loinc_code,
        display_name=display_name,
        value=value,
        unit=unit,
        reference_low=ref_low,
        reference_high=ref_high,
    )
