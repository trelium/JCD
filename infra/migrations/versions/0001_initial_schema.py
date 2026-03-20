"""initial schema

Revision ID: 0001
Revises:
Create Date: 2025-01-01 00:00:00.000000
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("job_id", UUID(as_uuid=True), primary_key=True),
        sa.Column("input_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("status", sa.String(32), nullable=False, default="pending"),
        sa.Column("patient_id", sa.String(128), nullable=True),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("parsed_payload", JSONB, nullable=True),
        sa.Column("validation_result", JSONB, nullable=True),
        sa.Column("analysis_result", JSONB, nullable=True),
        sa.Column("report_result", JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("ix_jobs_input_hash", "jobs", ["input_hash"])
    op.create_index("ix_jobs_status", "jobs", ["status"])

    op.create_table(
        "reference_ranges",
        sa.Column("loinc_code", sa.String(16), primary_key=True),
        sa.Column("display_name", sa.String(128), nullable=False),
        sa.Column("sex", sa.String(8), primary_key=True),
        sa.Column("age_min", sa.Integer, primary_key=True),
        sa.Column("age_max", sa.Integer, primary_key=True),
        sa.Column("p2_5", sa.Float, nullable=True),
        sa.Column("p5", sa.Float, nullable=True),
        sa.Column("p10", sa.Float, nullable=True),
        sa.Column("p25", sa.Float, nullable=True),
        sa.Column("p50", sa.Float, nullable=True),
        sa.Column("p75", sa.Float, nullable=True),
        sa.Column("p90", sa.Float, nullable=True),
        sa.Column("p95", sa.Float, nullable=True),
        sa.Column("p97_5", sa.Float, nullable=True),
        sa.Column("unit", sa.String(32), nullable=False),
    )
    op.create_index(
        "ix_refrange_loinc_sex_age",
        "reference_ranges",
        ["loinc_code", "sex", "age_min", "age_max"],
    )


def downgrade() -> None:
    op.drop_table("reference_ranges")
    op.drop_table("jobs")
