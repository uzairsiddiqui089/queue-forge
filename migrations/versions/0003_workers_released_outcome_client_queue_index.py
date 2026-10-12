"""workers table, 'released' attempt outcome, per-client queued index

Revision ID: 0003
Revises: 0002
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

OLD_OUTCOMES = "'succeeded', 'retryable_error', 'permanent_error', 'lease_expired'"
NEW_OUTCOMES = OLD_OUTCOMES + ", 'released'"


def upgrade() -> None:
    op.create_table(
        "workers",
        sa.Column("id", sa.String(length=100), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_jobs_client_queued",
        "jobs",
        ["client_id"],
        unique=False,
        postgresql_where=sa.text("status = 'queued'"),
    )
    # Allow the new attempt outcome. A CHECK constraint cannot be altered, only replaced.
    op.drop_constraint("ck_job_attempts_outcome", "job_attempts", type_="check")
    op.create_check_constraint(
        "ck_job_attempts_outcome",
        "job_attempts",
        f"outcome IS NULL OR outcome IN ({NEW_OUTCOMES})",
    )


def downgrade() -> None:
    # Attempts recorded as 'released' cannot satisfy the old constraint, so fold them into
    # 'lease_expired' (the closest old meaning: the attempt ended without a result).
    op.execute("UPDATE job_attempts SET outcome = 'lease_expired' WHERE outcome = 'released'")
    op.drop_constraint("ck_job_attempts_outcome", "job_attempts", type_="check")
    op.create_check_constraint(
        "ck_job_attempts_outcome",
        "job_attempts",
        f"outcome IS NULL OR outcome IN ({OLD_OUTCOMES})",
    )
    op.drop_index("ix_jobs_client_queued", table_name="jobs", postgresql_where=sa.text("status = 'queued'"))
    op.drop_table("workers")
