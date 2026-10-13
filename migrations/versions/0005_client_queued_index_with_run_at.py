"""per-client queued index that also carries run_at (for fair claiming)

Fair claiming looks, per client, for the oldest runnable queued job. The old index
(client_id only, queued rows) could not order by run_at, so it is replaced by one on
(client_id, run_at). It still serves the per-client queue count, because client_id is its
first column.

Revision ID: 0005
Revises: 0004
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_jobs_client_queued_run_at",
        "jobs",
        ["client_id", "run_at"],
        unique=False,
        postgresql_where=sa.text("status = 'queued'"),
    )
    op.drop_index(
        "ix_jobs_client_queued", table_name="jobs", postgresql_where=sa.text("status = 'queued'")
    )


def downgrade() -> None:
    op.create_index(
        "ix_jobs_client_queued",
        "jobs",
        ["client_id"],
        unique=False,
        postgresql_where=sa.text("status = 'queued'"),
    )
    op.drop_index(
        "ix_jobs_client_queued_run_at",
        table_name="jobs",
        postgresql_where=sa.text("status = 'queued'"),
    )
