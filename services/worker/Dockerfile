FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements/worker.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY services/worker/ services/worker/

ENV PYTHONPATH=/app
ENV ENV=production

# Default command overridden per-service in docker-compose / railway.toml
CMD ["python", "-m", "services.worker.validation_worker"]
