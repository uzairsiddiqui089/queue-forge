"""indexes for retention (finished jobs by status and age; idempotency keys by expiry and job)

Retention deletes the OLDEST finished jobs of one status, in small batches, and expired
idempotency keys. Without these indexes every batch would scan the whole table.

Revision ID: 0006
Revises: 0005
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

FINISHED = "status IN ('succeeded', 'failed', 'dead_lettered')"


def upgrade() -> None:
    op.create_index(
        "ix_jobs_finished_status_completed",
        "jobs",
        ["status", "completed_at"],
        unique=False,
        postgresql_where=sa.text(FINISHED),
    )
    op.create_index("ix_idempotency_keys_expires_at", "idempotency_keys", ["expires_at"])
    op.create_index("ix_idempotency_keys_job_id", "idempotency_keys", ["job_id"])


def downgrade() -> None:
    op.drop_index("ix_idempotency_keys_job_id", table_name="idempotency_keys")
    op.drop_index("ix_idempotency_keys_expires_at", table_name="idempotency_keys")
    op.drop_index(
        "ix_jobs_finished_status_completed", table_name="jobs", postgresql_where=sa.text(FINISHED)
    )
