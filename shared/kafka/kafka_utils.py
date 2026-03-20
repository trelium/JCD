"""
Postgres-based job queue — drop-in replacement for the Kafka-based transport.

Preserves the same architectural pattern (producer/consumer, stages, DLQ,
retry with back-off) but uses the existing Postgres jobs table as the
message bus instead of a Kafka broker.

Each pipeline stage is modelled as a status transition:
  pending      → claimed by validation worker
  validating   → claimed by analysis worker  (after validation done)
  analyzing    → claimed by report worker    (after analysis done)
  reporting    → complete

Workers poll for jobs in their input status, process them, and advance
the status. A failed job is retried up to max_retries times before being
marked dlq (dead-letter queue equivalent).

SELECT FOR UPDATE SKIP LOCKED ensures multiple worker replicas never
double-process the same job — this is the standard Postgres queue pattern
used in production by many systems (Sidekiq, River, Que, etc).
"""
from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db.database import AsyncSessionFactory, JobRow
from shared.models.models import JobStatus

log = structlog.get_logger()

POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "2"))


# ---------------------------------------------------------------------------
# Core polling loop
# ---------------------------------------------------------------------------

async def consume_loop_async(
    worker_name: str,
    input_status: str,
    handler: Callable,
    max_retries: int = 3,
) -> None:
    log.info("worker.started", worker=worker_name, polling_status=input_status)

    while True:
        try:
            async with AsyncSessionFactory() as session:
                # SELECT FOR UPDATE SKIP LOCKED — safe for N concurrent replicas
                stmt = (
                    select(JobRow)
                    .where(JobRow.status == input_status)
                    .order_by(JobRow.created_at)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                result = await session.execute(stmt)
                row = result.scalar_one_or_none()

                if row is None:
                    await session.rollback()
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue

                job_id = row.job_id
                attempt = _parse_attempt(row.error)
                log.info("worker.claimed", worker=worker_name, job_id=str(job_id))

                try:
                    await handler(row, session)
                    log.info("worker.complete", worker=worker_name, job_id=str(job_id))

                except Exception as exc:
                    attempt += 1
                    log.warning(
                        "worker.failed",
                        worker=worker_name,
                        job_id=str(job_id),
                        attempt=attempt,
                        error=str(exc),
                    )
                    wait = 0.5 * (2 ** (attempt - 1))
                    await asyncio.sleep(wait)

                    new_status = "dlq" if attempt >= max_retries else input_status
                    if new_status == "dlq":
                        log.error("worker.dlq", job_id=str(job_id), error=str(exc))

                    await session.execute(
                        update(JobRow)
                        .where(JobRow.job_id == job_id)
                        .values(
                            status=new_status,
                            error=f"[attempt {attempt}] {str(exc)}",
                            updated_at=datetime.utcnow(),
                        )
                    )
                    await session.commit()

        except Exception as exc:
            log.error("worker.loop_error", worker=worker_name, error=str(exc))
            await asyncio.sleep(5)


def consume_loop(
    worker_name: str,
    input_status: str,
    handler: Callable,
    max_retries: int = 3,
) -> None:
    """Sync entry point."""
    asyncio.run(consume_loop_async(worker_name, input_status, handler, max_retries))


# ---------------------------------------------------------------------------
# No-op stubs — keep existing imports working
# ---------------------------------------------------------------------------

def get_producer():
    return None

def get_consumer(group_id: str, topics: list[str]):
    return None

def ensure_topics(topics: list[str], **kwargs) -> None:
    pass

def publish(producer: Any, topic: str, key: str, payload: dict) -> None:
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_attempt(error: str | None) -> int:
    if not error:
        return 0
    try:
        if error.startswith("[attempt "):
            return int(error.split("]")[0].replace("[attempt ", ""))
    except Exception:
        pass
    return 0
