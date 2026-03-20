"""
FastAPI service — public-facing.

Routes:
  POST /upload          accepts FHIR JSON or XML, returns {job_id}
  GET  /status/{job_id} returns current job state + result when complete
  GET  /report/{job_id} returns rendered HTML report
  GET  /health          liveness probe
  GET  /ready           readiness probe (checks DB + Kafka)
"""
from __future__ import annotations

import base64
import hashlib
import os
import sys
import uuid
from contextlib import asynccontextmanager
from typing import Annotated

import structlog
from confluent_kafka import Producer
from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from shared.db.database import AsyncSessionFactory, JobRow, create_all_tables
from shared.kafka.kafka_utils import ensure_topics, get_producer, publish
from shared.logging_config import configure_logging
from shared.models.models import Job, JobStatus, KafkaIngestMessage

configure_logging()
log = structlog.get_logger()

TOPIC_INGEST = "fhir-ingest"
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "10"))
limiter = Limiter(key_func=get_remote_address)

# ---------------------------------------------------------------------------
# Application lifecycle
# ---------------------------------------------------------------------------

_producer: Producer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _producer
    await create_all_tables()
    ensure_topics([TOPIC_INGEST, TOPIC_INGEST + ".dlq"])
    _producer = get_producer()
    log.info("api.started")
    yield
    if _producer:
        _producer.flush(timeout=10)
    log.info("api.shutdown")


app = FastAPI(
    title="Biomarker Pipeline API",
    version="1.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

async def get_db() -> AsyncSession:
    async with AsyncSessionFactory() as session:
        yield session


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------

class UploadResponse(BaseModel):
    job_id: str
    status: str
    message: str


class StatusResponse(BaseModel):
    job_id: str
    status: str
    patient_id: str | None
    risk_tier: str | None
    error: str | None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.post("/upload", response_model=UploadResponse, status_code=202)
@limiter.limit("20/minute")
async def upload_fhir(
    request: Request,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
) -> UploadResponse:
    """Accept a FHIR Bundle in JSON or XML format."""
    content_type = file.content_type or ""
    filename = file.filename or ""

    if not (
        "json" in content_type
        or "xml" in content_type
        or filename.endswith(".json")
        or filename.endswith(".xml")
    ):
        raise HTTPException(400, "File must be FHIR JSON or XML")

    raw = await file.read()
    if len(raw) > MAX_UPLOAD_MB * 1024 * 1024:
        raise HTTPException(413, f"File exceeds {MAX_UPLOAD_MB} MB limit")

    source_format = "xml" if ("xml" in content_type or filename.endswith(".xml")) else "json"
    input_hash = hashlib.sha256(raw).hexdigest()

    # Idempotency: return existing job if same file was uploaded before
    existing = await db.execute(select(JobRow).where(JobRow.input_hash == input_hash))
    existing_row = existing.scalar_one_or_none()
    if existing_row:
        log.info("api.duplicate_upload", job_id=str(existing_row.job_id))
        return UploadResponse(
            job_id=str(existing_row.job_id),
            status=existing_row.status,
            message="Duplicate upload — returning existing job",
        )

    job_id = uuid.uuid4()
    job_row = JobRow(
        job_id=job_id,
        input_hash=input_hash,
        status=JobStatus.PENDING,
    )
    db.add(job_row)
    await db.commit()

    msg = KafkaIngestMessage(
        job_id=str(job_id),
        input_hash=input_hash,
        raw_bytes_b64=base64.b64encode(raw).decode(),
        source_format=source_format,
    )
    publish(_producer, TOPIC_INGEST, str(job_id), msg.model_dump())
    log.info("api.upload_accepted", job_id=str(job_id), bytes=len(raw))

    return UploadResponse(
        job_id=str(job_id),
        status="pending",
        message="File accepted. Poll /status/{job_id} for updates.",
    )


@app.get("/status/{job_id}", response_model=StatusResponse)
async def get_status(
    job_id: str,
    db: AsyncSession = Depends(get_db),
) -> StatusResponse:
    try:
        uid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(400, "Invalid job_id format")

    row = await db.get(JobRow, uid)
    if not row:
        raise HTTPException(404, "Job not found")

    risk_tier = None
    if row.analysis_result:
        risk_tier = row.analysis_result.get("risk_tier")

    return StatusResponse(
        job_id=job_id,
        status=row.status,
        patient_id=row.patient_id,
        risk_tier=risk_tier,
        error=row.error,
    )


@app.get("/report/{job_id}", response_class=HTMLResponse)
async def get_report(
    job_id: str,
    db: AsyncSession = Depends(get_db),
) -> HTMLResponse:
    try:
        uid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(400, "Invalid job_id format")

    row = await db.get(JobRow, uid)
    if not row:
        raise HTTPException(404, "Job not found")
    if row.status != JobStatus.COMPLETE:
        raise HTTPException(425, f"Report not ready yet — status: {row.status}")
    if not row.report_result or not row.report_result.get("html_report"):
        raise HTTPException(500, "Report data missing")

    return HTMLResponse(content=row.report_result["html_report"])


# ---------------------------------------------------------------------------
# Health / readiness
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/ready")
async def ready(db: AsyncSession = Depends(get_db)) -> JSONResponse:
    issues: list[str] = []

    # Check DB
    try:
        await db.execute(select(1))
    except Exception as exc:
        issues.append(f"db: {exc}")

    # Check Kafka producer
    if _producer is None:
        issues.append("kafka: producer not initialised")

    if issues:
        return JSONResponse({"status": "not ready", "issues": issues}, status_code=503)
    return JSONResponse({"status": "ready"})


# ---------------------------------------------------------------------------
# Simple HTML upload UI
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse(content="""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Biomarker Pipeline</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: #f8f9fa; color: #212529; min-height: 100vh; display: flex; align-items: center; justify-content: center; }
  .card { background: white; border-radius: 12px; padding: 2.5rem; max-width: 520px; width: 100%; box-shadow: 0 2px 16px rgba(0,0,0,0.08); }
  h1 { font-size: 1.4rem; font-weight: 600; margin-bottom: 0.25rem; }
  p.sub { color: #6c757d; font-size: 0.9rem; margin-bottom: 1.75rem; }
  label { display: block; font-size: 0.85rem; font-weight: 500; margin-bottom: 0.4rem; }
  .dropzone { border: 2px dashed #dee2e6; border-radius: 8px; padding: 2rem; text-align: center; cursor: pointer; transition: border-color .2s; background: #fafafa; }
  .dropzone:hover, .dropzone.over { border-color: #0d6efd; background: #f0f4ff; }
  .dropzone input { display: none; }
  .dropzone .hint { font-size: 0.82rem; color: #6c757d; margin-top: 0.5rem; }
  .filename { margin-top: 0.75rem; font-size: 0.85rem; color: #198754; font-weight: 500; }
  button { display: block; width: 100%; margin-top: 1.25rem; padding: 0.75rem; background: #0d6efd; color: white; border: none; border-radius: 8px; font-size: 1rem; font-weight: 500; cursor: pointer; transition: background .2s; }
  button:hover { background: #0b5ed7; }
  button:disabled { background: #adb5bd; cursor: default; }
  #result { margin-top: 1.25rem; padding: 1rem; border-radius: 8px; font-size: 0.88rem; display: none; }
  #result.ok { background: #d1e7dd; color: #0a3622; }
  #result.err { background: #f8d7da; color: #58151c; }
  #result.pending { background: #fff3cd; color: #664d03; }
  .job-link { margin-top: 0.5rem; }
  .job-link a { color: inherit; font-weight: 600; }
  #poll-status { margin-top: 0.5rem; font-style: italic; }
</style>
</head>
<body>
<div class="card">
  <h1>Biomarker Report Pipeline</h1>
  <p class="sub">Upload a FHIR Bundle (JSON or XML) to generate a clinical blood panel report.</p>
  <label>FHIR file</label>
  <div class="dropzone" id="dz">
    <input type="file" id="file-input" accept=".json,.xml,application/json,application/xml,text/xml">
    <div>Drop file here or <strong>click to browse</strong></div>
    <div class="hint">FHIR Bundle — JSON or XML · max 10 MB</div>
    <div class="filename" id="fname"></div>
  </div>
  <button id="submit-btn" disabled>Generate report</button>
  <div id="result"></div>
</div>
<script>
const dz = document.getElementById('dz');
const fi = document.getElementById('file-input');
const btn = document.getElementById('submit-btn');
const res = document.getElementById('result');
const fname = document.getElementById('fname');
let selectedFile = null;

dz.addEventListener('click', () => fi.click());
dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('over'); });
dz.addEventListener('dragleave', () => dz.classList.remove('over'));
dz.addEventListener('drop', e => {
  e.preventDefault(); dz.classList.remove('over');
  const f = e.dataTransfer.files[0]; if (f) setFile(f);
});
fi.addEventListener('change', () => { if (fi.files[0]) setFile(fi.files[0]); });

function setFile(f) {
  selectedFile = f;
  fname.textContent = f.name + ' (' + (f.size/1024).toFixed(1) + ' KB)';
  btn.disabled = false;
}

btn.addEventListener('click', async () => {
  if (!selectedFile) return;
  btn.disabled = true;
  res.style.display = 'none';
  const form = new FormData();
  form.append('file', selectedFile);
  try {
    const r = await fetch('/upload', { method: 'POST', body: form });
    const data = await r.json();
    if (!r.ok) { showErr(data.detail || 'Upload failed'); btn.disabled = false; return; }
    showPending(data.job_id);
    poll(data.job_id);
  } catch(e) { showErr('Network error'); btn.disabled = false; }
});

function showPending(jobId) {
  res.className = 'pending'; res.style.display = 'block';
  res.innerHTML = 'Processing… <div id="poll-status">Waiting for pipeline...</div>'
    + '<div class="job-link">Job ID: <a href="/status/' + jobId + '" target="_blank">' + jobId + '</a></div>';
}

function showErr(msg) {
  res.className = 'err'; res.style.display = 'block';
  res.textContent = '⚠ ' + msg;
}

async function poll(jobId, attempt=0) {
  await new Promise(r => setTimeout(r, 2000 + attempt * 1000));
  try {
    const r = await fetch('/status/' + jobId);
    const data = await r.json();
    const ps = document.getElementById('poll-status');
    if (ps) ps.textContent = 'Status: ' + data.status + (data.risk_tier ? ' · Risk: ' + data.risk_tier : '');
    if (data.status === 'complete') {
      res.className = 'ok'; res.style.display = 'block';
      res.innerHTML = '✓ Report ready — Risk tier: <strong>' + (data.risk_tier||'—') + '</strong>'
        + '<div class="job-link"><a href="/report/' + jobId + '" target="_blank">Open full report →</a></div>';
    } else if (data.status === 'failed') {
      showErr('Pipeline failed: ' + (data.error || 'unknown error'));
    } else {
      if (attempt < 60) poll(jobId, attempt + 1);
    }
  } catch(e) { if (attempt < 60) poll(jobId, attempt+1); }
}
</script>
</body>
</html>
""")
