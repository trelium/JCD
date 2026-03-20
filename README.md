# Biomarker Pipeline

An end-to-end production pipeline that accepts FHIR-formatted blood panel files, validates biomarkers against NHANES population reference data, scores cardiovascular/metabolic risk, and generates GP-ready clinical reports using Claude.

## Architecture

```
Upload (FHIR JSON/XML)
        │
        ▼
  FastAPI /upload  ──────────────────────────────────────┐
        │                                                 │
        │ Kafka: fhir-ingest                              │
        ▼                                                 │
  Validation worker (×N)                                  │
    · Parse FHIR                                          │
    · Check vs NHANES percentiles                         │
    · Flag anomalies                                      │
        │                                                 │
        │ Kafka: fhir-validated                           │
        ▼                                                 │
  Analysis worker (×N)                          Postgres  │
    · Risk scoring                               (jobs +  │
    · Cohort percentiles                          refs)   │
    · Plotly chart data                                   │
        │                                                 │
        │ Kafka: fhir-analysed                            │
        ▼                                                 │
  Report worker                                           │
    · Claude API narrative                                │
    · HTML report render                                  │
        │                                                 │
        └──────────────── Job COMPLETE ───────────────────┘

Client polls GET /status/{job_id}
Client fetches GET /report/{job_id}  → rendered HTML
```

**Production readiness features:**
- Horizontal scaling: add worker replicas via `--scale` or Railway replica count
- Fault tolerance: Kafka retains messages; workers retry 3× with exponential back-off then route to `.dlq` topic
- Idempotency: duplicate uploads (same SHA-256) return cached job immediately
- Observability: structured JSON logs via `structlog` on every stage transition
- Health/readiness: `/health` (liveness) and `/ready` (checks DB + Kafka) on the API
- Schema validation: Pydantic v2 on all internal models; FHIR schema validated by `fhir.resources`
- DB migrations: Alembic with async support
- Rate limiting: 20 uploads/minute per IP via SlowAPI

---

## Local development

### Prerequisites
- Docker + Docker Compose
- Python 3.12 (for running outside Docker)

### Start everything

```bash
# Clone and enter the repo
git clone <your-repo>
cd biomarker-pipeline

# Copy and fill in your Anthropic API key (optional — reports work without it, narrative is skipped)
cp .env.example .env
# edit .env and set ANTHROPIC_API_KEY=sk-ant-...

# Build and start all services
docker compose up --build

# First run: wait for nhanes-loader to finish seeding (~5-10 min, downloads CDC data)
docker compose logs -f nhanes-loader
```

Services will be available at:
- **Web UI**: http://localhost:8000
- **API docs**: http://localhost:8000/docs
- **Redpanda console**: http://localhost:8080

### Test with the sample file

```bash
# Upload the included sample FHIR bundle
curl -X POST http://localhost:8000/upload \
  -F "file=@sample_fhir_bundle.json"

# Response: {"job_id": "...", "status": "pending", ...}
# Poll status:
curl http://localhost:8000/status/<job_id>

# Once status=complete, open the report:
open http://localhost:8000/report/<job_id>
```

### Scale workers locally

```bash
docker compose up --scale validation-worker=3 --scale analysis-worker=2
```

---

## Railway deployment (free tier)

### One-time setup

1. **Push repo to GitHub**

2. **Create Railway project**
   - Go to https://railway.app → New Project → Deploy from GitHub repo
   - Select your repository

3. **Add database plugins** (Railway dashboard → New → Database):
   - PostgreSQL
   - (For Kafka/Redpanda: use Upstash Kafka from the Railway Marketplace — free tier available)

4. **Set environment variables** on each service (Settings → Variables):

   | Variable | Value |
   |---|---|
   | `DATABASE_URL` | Copy from Railway Postgres plugin (change `postgresql://` to `postgresql+asyncpg://`) |
   | `KAFKA_BOOTSTRAP_SERVERS` | From Upstash Kafka plugin |
   | `ANTHROPIC_API_KEY` | Your Anthropic key |
   | `ENV` | `production` |

5. **Run the NHANES seeder once**
   - In Railway dashboard, open the `nhanes-loader` service
   - Click "Deploy" — it runs once and exits
   - Check logs to confirm seeding completed successfully

6. **Verify the API is live**
   - Open the public URL Railway assigns to the `api` service
   - You should see the upload UI

### Scaling on Railway

In the Railway dashboard, select any worker service → Settings → Replicas → increase to 2 or 3.
Kafka consumer group coordination handles the load distribution automatically.

### Running Alembic migrations manually

```bash
# From your local machine with DATABASE_URL set
pip install alembic asyncpg sqlalchemy
alembic upgrade head
```

---

## Project structure

```
biomarker-pipeline/
├── docker-compose.yml          # Local dev stack
├── railway.toml                # Railway service definitions
├── alembic.ini                 # DB migration config
├── sample_fhir_bundle.json     # Test file
│
├── requirements/
│   ├── api.txt
│   ├── worker.txt
│   └── nhanes.txt
│
├── shared/                     # Code shared across all services
│   ├── models/models.py        # Pydantic data models
│   ├── kafka/kafka_utils.py    # Producer/consumer + retry logic
│   ├── db/database.py          # SQLAlchemy async engine + ORM models
│   └── logging_config.py       # structlog JSON configuration
│
├── services/
│   ├── api/
│   │   ├── main.py             # FastAPI app + upload/status/report routes
│   │   └── Dockerfile
│   │
│   ├── worker/
│   │   ├── fhir_parser.py      # FHIR JSON + XML → internal models
│   │   ├── validation_worker.py # Stage 1: parse + validate vs NHANES
│   │   ├── analysis_worker.py  # Stage 2: risk scoring + Plotly charts
│   │   ├── report_worker.py    # Stage 3: Claude narrative + HTML render
│   │   └── Dockerfile
│   │
│   └── nhanes/
│       ├── loader.py           # Seeds reference_ranges from CDC data
│       └── Dockerfile
│
└── infra/
    └── migrations/
        ├── env.py              # Alembic async setup
        └── versions/
            └── 0001_initial_schema.py
```

---

## FHIR input format

The pipeline accepts a FHIR R4 Bundle containing:
- One `Patient` resource (for age/sex stratification)
- One or more `Observation` resources with LOINC-coded blood biomarkers

See `sample_fhir_bundle.json` for a complete example.

Supported LOINC codes and biomarkers: Total Cholesterol (2093-3), LDL (13457-7), HDL (2085-9), Triglycerides (3043-7), Glucose (2345-7), HbA1c (4548-4), hsCRP (30522-7), Creatinine (2157-6), BUN (3094-0), ALT (1742-6), AST (1920-8), Ferritin (2276-4), Hemoglobin (718-7), WBC (6690-2), Platelets (777-3), Sodium (2947-0), Potassium (6298-4), and more.

---

## Reference data

Population reference ranges are derived from the **NHANES 2017–2018** dataset (National Health and Nutrition Examination Survey, CDC). Percentile distributions are stratified by sex (male/female) and age band (0–17, 18–29, 30–39, 40–49, 50–59, 60–69, 70+). Data is downloaded directly from the CDC public server at first run — no license or API key required.

---

## Extending the pipeline

**Add a new biomarker**: add an entry to `BIOMARKER_MAP` in `services/nhanes/loader.py` with the LOINC code, NHANES variable name, XPT file URL, and unit. Re-run the loader.

**Add a new pipeline stage**: create a new worker module, add a new Kafka topic in `kafka_utils.ensure_topics`, subscribe the new worker to the upstream topic, publish to a new downstream topic, add the service to `docker-compose.yml` and `railway.toml`.

**Swap Claude model**: change `CLAUDE_MODEL` in `services/worker/report_worker.py`.
