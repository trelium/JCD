#!/usr/bin/env python3
"""
One-shot service that downloads NHANES lab data from the CDC public server,
computes per-biomarker percentile distributions stratified by age and sex,
and seeds the reference_ranges table in Postgres.

Run once before workers start (Railway start command or a one-off dyno).

NHANES data is hosted at:
  https://wwwn.cdc.gov/Nchs/Nhanes/<cycle>/<component>.XPT

We pull the 2017-2018 cycle (pre-COVID, last complete cycle).
"""
from __future__ import annotations

import asyncio
import io
import os
import sys

import httpx
import numpy as np
import pandas as pd
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from shared.db.database import AsyncSessionFactory, ReferenceRangeRow, create_all_tables
from shared.logging_config import configure_logging

configure_logging()
log = structlog.get_logger()

# ---------------------------------------------------------------------------
# NHANES file manifest
# LOINC codes mapped to (NHANES_variable, component_XPT_url, unit)
# ---------------------------------------------------------------------------

BASE = "https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018"

BIOMARKER_MAP = {
    # LOINC          display_name                   nhanes_var    xpt_file               unit
    "2093-3":  ("Total Cholesterol",               "LBXTC",   f"{BASE}/TCHOL_J.XPT",  "mg/dL"),
    "13457-7": ("LDL Cholesterol",                 "LBDLDL",  f"{BASE}/TRIGLY_J.XPT", "mg/dL"),
    "2085-9":  ("HDL Cholesterol",                 "LBDHDD",  f"{BASE}/HDL_J.XPT",    "mg/dL"),
    "3043-7":  ("Triglycerides",                   "LBXTR",   f"{BASE}/TRIGLY_J.XPT", "mg/dL"),
    "2345-7":  ("Glucose (fasting)",               "LBXGLU",  f"{BASE}/GLU_J.XPT",    "mg/dL"),
    "4548-4":  ("HbA1c",                           "LBXGH",   f"{BASE}/GHB_J.XPT",    "%"),
    "30522-7": ("C-Reactive Protein (hs)",         "LBXHSCRP",f"{BASE}/HSCRP_J.XPT",  "mg/L"),
    "2157-6":  ("Creatinine",                      "LBXSCR",  f"{BASE}/BIOPRO_J.XPT", "mg/dL"),
    "3094-0":  ("BUN",                             "LBXSBU",  f"{BASE}/BIOPRO_J.XPT", "mg/dL"),
    "1742-6":  ("ALT",                             "LBXSATSI",f"{BASE}/BIOPRO_J.XPT", "U/L"),
    "1920-8":  ("AST",                             "LBXSASSI",f"{BASE}/BIOPRO_J.XPT", "U/L"),
    "2276-4":  ("Ferritin",                        "LBXFER",  f"{BASE}/FERTIN_J.XPT", "ng/mL"),
    "718-7":   ("Hemoglobin",                      "LBXHGB",  f"{BASE}/CBC_J.XPT",    "g/dL"),
    "787-2":   ("MCV",                             "LBXMCVSI",f"{BASE}/CBC_J.XPT",    "fL"),
    "6690-2":  ("WBC",                             "LBXWBCSI",f"{BASE}/CBC_J.XPT",    "10^3/uL"),
    "777-3":   ("Platelets",                       "LBXPLTSI",f"{BASE}/CBC_J.XPT",    "10^3/uL"),
    "2947-0":  ("Sodium",                          "LBXSNASI",f"{BASE}/BIOPRO_J.XPT", "mmol/L"),
    "6298-4":  ("Potassium",                       "LBXSKSI", f"{BASE}/BIOPRO_J.XPT", "mmol/L"),
    "17861-6": ("Calcium",                         "LBXSCASI",f"{BASE}/BIOPRO_J.XPT", "mg/dL"),
    "2532-0":  ("LDH",                             "LBXSLDSI",f"{BASE}/BIOPRO_J.XPT", "U/L"),
    "14627-4": ("Bicarbonate",                     "LBXSC3SI",f"{BASE}/BIOPRO_J.XPT", "mmol/L"),
    "2069-3":  ("Chloride",                        "LBXSCLSI",f"{BASE}/BIOPRO_J.XPT", "mmol/L"),
    "2885-2":  ("Total Protein",                   "LBXSTP",  f"{BASE}/BIOPRO_J.XPT", "g/dL"),
    "1751-7":  ("Albumin",                         "LBXSAL",  f"{BASE}/BIOPRO_J.XPT", "g/dL"),
    "1975-2":  ("Total Bilirubin",                 "LBXSTB",  f"{BASE}/BIOPRO_J.XPT", "mg/dL"),
    "6768-6":  ("Alkaline Phosphatase",            "LBXSAPSI",f"{BASE}/BIOPRO_J.XPT", "U/L"),
    "2000-8":  ("Phosphorus",                      "LBXSPH",  f"{BASE}/BIOPRO_J.XPT", "mg/dL"),
    "2498-4":  ("Iron",                            "LBXSIR",  f"{BASE}/BIOPRO_J.XPT", "ug/dL"),
    "14804-9": ("Uric Acid",                       "LBXSUA",  f"{BASE}/BIOPRO_J.XPT", "mg/dL"),
}

DEMOGRAPHICS_URL = f"{BASE}/DEMO_J.XPT"

AGE_BINS = [(0,17),(18,29),(30,39),(40,49),(50,59),(60,69),(70,120)]
PERCENTILES = [2.5, 5, 10, 25, 50, 75, 90, 95, 97.5]


async def download_xpt(url: str) -> pd.DataFrame:
    log.info("nhanes.downloading", url=url)
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.get(url)
        r.raise_for_status()
    df = pd.read_sas(io.BytesIO(r.content), format="xport")
    log.info("nhanes.downloaded", url=url, rows=len(df))
    return df


async def load_demographics() -> pd.DataFrame:
    df = await download_xpt(DEMOGRAPHICS_URL)
    # SEQN=participant id, RIDAGEYR=age, RIAGENDR=1 male/2 female
    demo = df[["SEQN","RIDAGEYR","RIAGENDR"]].copy()
    demo.columns = ["seqn","age","sex_code"]
    demo["sex"] = demo["sex_code"].map({1:"male",2:"female"}).fillna("unknown")
    return demo.set_index("seqn")


async def seed_biomarker(
    session: AsyncSession,
    loinc: str,
    display_name: str,
    nhanes_var: str,
    xpt_url: str,
    unit: str,
    demo: pd.DataFrame,
    xpt_cache: dict[str, pd.DataFrame],
) -> None:
    if xpt_url not in xpt_cache:
        xpt_cache[xpt_url] = await download_xpt(xpt_url)
    df = xpt_cache[xpt_url]

    if nhanes_var not in df.columns:
        log.warning("nhanes.var_missing", var=nhanes_var, url=xpt_url)
        return

    merged = df[["SEQN", nhanes_var]].join(demo, on="SEQN").dropna(subset=[nhanes_var, "age", "sex"])
    merged = merged.rename(columns={nhanes_var: "value"})
    merged["value"] = pd.to_numeric(merged["value"], errors="coerce")
    merged = merged.dropna(subset=["value"])

    rows_to_add: list[ReferenceRangeRow] = []
    for sex in ("male", "female", "all"):
        subset = merged if sex == "all" else merged[merged["sex"] == sex]
        for age_min, age_max in AGE_BINS:
            cohort = subset[(subset["age"] >= age_min) & (subset["age"] <= age_max)]["value"]
            if len(cohort) < 30:
                continue
            pctls = np.percentile(cohort, PERCENTILES)
            rows_to_add.append(
                ReferenceRangeRow(
                    loinc_code=loinc,
                    display_name=display_name,
                    sex=sex,
                    age_min=age_min,
                    age_max=age_max,
                    p2_5=float(pctls[0]),
                    p5=float(pctls[1]),
                    p10=float(pctls[2]),
                    p25=float(pctls[3]),
                    p50=float(pctls[4]),
                    p75=float(pctls[5]),
                    p90=float(pctls[6]),
                    p95=float(pctls[7]),
                    p97_5=float(pctls[8]),
                    unit=unit,
                )
            )

    for row in rows_to_add:
        await session.merge(row)    # upsert
    await session.commit()
    log.info("nhanes.seeded", loinc=loinc, display_name=display_name, rows=len(rows_to_add))


async def main() -> None:
    await create_all_tables()
    demo = await load_demographics()
    xpt_cache: dict[str, pd.DataFrame] = {}

    async with AsyncSessionFactory() as session:
        for loinc, (display_name, nhanes_var, xpt_url, unit) in BIOMARKER_MAP.items():
            try:
                await seed_biomarker(
                    session, loinc, display_name, nhanes_var, xpt_url, unit, demo, xpt_cache
                )
            except Exception as exc:
                log.error("nhanes.seed_failed", loinc=loinc, error=str(exc))

    log.info("nhanes.seed_complete", biomarkers=len(BIOMARKER_MAP))


if __name__ == "__main__":
    asyncio.run(main())
