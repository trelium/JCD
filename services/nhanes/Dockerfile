FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements/nhanes.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY services/nhanes/ services/nhanes/

ENV PYTHONPATH=/app
ENV ENV=production

CMD ["python", "services/nhanes/loader.py"]
