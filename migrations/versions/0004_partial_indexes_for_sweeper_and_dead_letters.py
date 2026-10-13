"""partial indexes for the lease sweeper and the dead-letter listing

Chosen from measurements (docs/benchmarking.md), not guesses. With 1,000,000 finished jobs:
the sweeper's query took 230-277 ms (a full table scan) and the dead-letter listing took
131-152 ms; with these indexes they take well under 1 ms. The indexes only contain the few
rows in the 'running' / 'dead_lettered' states, so they stay tiny.

Revision ID: 0004
Revises: 0003
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_jobs_running_lease",
        "jobs",
        ["lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("status = 'running'"),
    )
    op.create_index(
        "ix_jobs_dead_lettered_completed",
        "jobs",
        [sa.text("completed_at DESC"), "id"],
        unique=False,
        postgresql_where=sa.text("status = 'dead_lettered'"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_jobs_dead_lettered_completed",
        table_name="jobs",
        postgresql_where=sa.text("status = 'dead_lettered'"),
    )
    op.drop_index(
        "ix_jobs_running_lease", table_name="jobs", postgresql_where=sa.text("status = 'running'")
    )
