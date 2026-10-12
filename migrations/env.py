"""Alembic environment. The URL comes from `sqlalchemy.url` if a caller set it
(tests do), otherwise from the application settings (DATABASE_URL)."""

from alembic import context
from sqlalchemy import create_engine

from queueforge.config import get_settings
from queueforge.db import models  # noqa: F401  (registers the tables on Base.metadata)
from queueforge.db.base import Base

config = context.config
target_metadata = Base.metadata


def _url() -> str:
    return config.get_main_option("sqlalchemy.url") or get_settings().database_url


def run_migrations_offline() -> None:
    context.configure(url=_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_url())
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
