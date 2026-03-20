"""
Parse a FHIR Bundle (JSON or XML) into our internal ParsedFHIRPayload.

No fhir.resources dependency — plain json/xml parsing only.
Supports FHIR R4 Bundles containing Patient + Observation resources.
"""
from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from datetime import datetime

import structlog

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
    data = json.loads(raw)
    entries = data.get("entry", [])
    demographics = PatientDemographics(patient_id="unknown")
    observations: list[BiomarkerObservation] = []

    for entry in entries:
        resource = entry.get("resource", {})
        rtype = resource.get("resourceType", "")
        if rtype == "Patient":
            demographics = _demographics_from_json(resource)
        elif rtype == "Observation":
            obs = _observation_from_json(resource)
            if obs:
                observations.append(obs)

    return ParsedFHIRPayload(
        demographics=demographics,
        observations=observations,
        raw_resource_count=len(entries),
        source_format="json",
    )


def _demographics_from_json(r: dict) -> PatientDemographics:
    patient_id = r.get("id", "unknown")
    age: int | None = None
    sex: str | None = None

    bd = r.get("birthDate")
    if bd:
        try:
            age = (datetime.utcnow() - datetime.strptime(bd, "%Y-%m-%d")).days // 365
        except Exception:
            pass

    g = r.get("gender", "")
    if g in ("male", "female"):
        sex = g

    return PatientDemographics(patient_id=patient_id, age=age, sex=sex)


def _observation_from_json(r: dict) -> BiomarkerObservation | None:
    loinc_code: str | None = None
    display_name = "Unknown"

    for coding in r.get("code", {}).get("coding", []):
        if "loinc" in (coding.get("system") or "").lower():
            loinc_code = coding.get("code")
            display_name = coding.get("display", display_name)
            break

    if not loinc_code:
        return None

    vq = r.get("valueQuantity", {})
    try:
        value = float(vq.get("value", ""))
    except (TypeError, ValueError):
        return None

    unit = vq.get("unit", "")

    ref_low = ref_high = None
    for rr in r.get("referenceRange", []):
        try:
            ref_low = float(rr.get("low", {}).get("value", ""))
        except (TypeError, ValueError):
            pass
        try:
            ref_high = float(rr.get("high", {}).get("value", ""))
        except (TypeError, ValueError):
            pass

    effective: datetime | None = None
    ed = r.get("effectiveDateTime")
    if ed:
        try:
            effective = datetime.fromisoformat(ed.replace("Z", "+00:00"))
        except Exception:
            pass

    return BiomarkerObservation(
        loinc_code=loinc_code,
        display_name=display_name,
        value=value,
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

    def tag(name: str) -> str:
        return f"{{{FHIR_NS}}}{name}"

    def val(el) -> str | None:
        return el.get("value") if el is not None else None

    demographics = PatientDemographics(patient_id="unknown")
    observations: list[BiomarkerObservation] = []
    count = 0

    for entry in root.findall(f".//{tag('entry')}"):
        count += 1
        patient_el = entry.find(f".//{tag('Patient')}")
        obs_el = entry.find(f".//{tag('Observation')}")
        if patient_el is not None:
            demographics = _demographics_from_xml(patient_el, tag, val)
        elif obs_el is not None:
            obs = _observation_from_xml(obs_el, tag, val)
            if obs:
                observations.append(obs)

    return ParsedFHIRPayload(
        demographics=demographics,
        observations=observations,
        raw_resource_count=count,
        source_format="xml",
    )


def _demographics_from_xml(el, tag, val) -> PatientDemographics:
    patient_id = val(el.find(tag("id"))) or "unknown"
    age: int | None = None
    sex: str | None = None

    bd = val(el.find(tag("birthDate")))
    if bd:
        try:
            age = (datetime.utcnow() - datetime.strptime(bd, "%Y-%m-%d")).days // 365
        except Exception:
            pass

    g = val(el.find(tag("gender"))) or ""
    if g in ("male", "female"):
        sex = g

    return PatientDemographics(patient_id=patient_id, age=age, sex=sex)


def _observation_from_xml(el, tag, val) -> BiomarkerObservation | None:
    loinc_code: str | None = None
    display_name = "Unknown"

    for coding in el.findall(f".//{tag('coding')}"):
        system = val(coding.find(tag("system"))) or ""
        if "loinc" in system.lower():
            loinc_code = val(coding.find(tag("code")))
            display_name = val(coding.find(tag("display"))) or display_name
            break

    if not loinc_code:
        return None

    vq = el.find(f".//{tag('valueQuantity')}")
    if vq is None:
        return None

    try:
        value = float(val(vq.find(tag("value"))) or "")
    except (TypeError, ValueError):
        return None

    unit = val(vq.find(tag("unit"))) or ""

    ref_low = ref_high = None
    for rr in el.findall(f".//{tag('referenceRange')}"):
        try:
            ref_low = float(val(rr.find(f".//{tag('low')}/{tag('value')}")) or "")
        except (TypeError, ValueError):
            pass
        try:
            ref_high = float(val(rr.find(f".//{tag('high')}/{tag('value')}")) or "")
        except (TypeError, ValueError):
            pass

    return BiomarkerObservation(
        loinc_code=loinc_code,
        display_name=display_name,
        value=value,
        unit=unit,
        reference_low=ref_low,
        reference_high=ref_high,
    )
