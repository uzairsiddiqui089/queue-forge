"""Database engine construction and a lightweight connectivity check."""

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def make_engine(database_url: str) -> Engine:
    # pool_pre_ping discards dead connections instead of handing them to callers.
    return create_engine(database_url, pool_pre_ping=True, connect_args={"connect_timeout": 3})


def database_is_reachable(engine: Engine) -> bool:
    """Return True if a trivial query succeeds within a short timeout."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except SQLAlchemyError:
        return False
    return True
