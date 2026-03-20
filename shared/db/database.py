"""
Async SQLAlchemy engine + session factory.
Table definitions live here; Alembic autogenerates migrations from them.
"""
from __future__ import annotations

import os
from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Enum, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/biomarker",
)

engine = create_async_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,             # detect stale connections
    echo=False,
)

AsyncSessionFactory = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Jobs table
# ---------------------------------------------------------------------------

class JobRow(Base):
    __tablename__ = "jobs"

    job_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    input_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(32), index=True, default="pending")
    patient_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    parsed_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    validation_result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    analysis_result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    report_result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


# ---------------------------------------------------------------------------
# NHANES reference ranges table
# ---------------------------------------------------------------------------

class ReferenceRangeRow(Base):
    __tablename__ = "reference_ranges"

    loinc_code: Mapped[str] = mapped_column(String(16), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(128))
    sex: Mapped[str] = mapped_column(String(8), primary_key=True)   # male/female/all
    age_min: Mapped[int] = mapped_column(primary_key=True)
    age_max: Mapped[int] = mapped_column(primary_key=True)
    p2_5: Mapped[float | None] = mapped_column(nullable=True)        # 2.5th percentile
    p5: Mapped[float | None] = mapped_column(nullable=True)
    p10: Mapped[float | None] = mapped_column(nullable=True)
    p25: Mapped[float | None] = mapped_column(nullable=True)
    p50: Mapped[float | None] = mapped_column(nullable=True)         # median
    p75: Mapped[float | None] = mapped_column(nullable=True)
    p90: Mapped[float | None] = mapped_column(nullable=True)
    p95: Mapped[float | None] = mapped_column(nullable=True)
    p97_5: Mapped[float | None] = mapped_column(nullable=True)       # 97.5th percentile
    unit: Mapped[str] = mapped_column(String(32))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def create_all_tables() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session() -> AsyncSession:
    async with AsyncSessionFactory() as session:
        yield session
