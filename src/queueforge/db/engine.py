"""Database engine construction and a lightweight connectivity check."""

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from queueforge.config import Settings


def make_engine(
    database_url: str,
    *,
    pool_size: int = 5,
    max_overflow: int = 10,
    pool_timeout: float = 30.0,
    statement_timeout_seconds: float = 30.0,
    lock_timeout_seconds: float = 5.0,
    idle_in_transaction_timeout_seconds: float = 60.0,
) -> Engine:
    """Create the engine with explicit pool limits and server-side timeouts.

    The timeouts are set on every new connection, so a stuck query, a lock wait or an
    abandoned open transaction cannot hold a connection (or a row lock) forever.
    """
    options = (
        f"-c statement_timeout={int(statement_timeout_seconds * 1000)} "
        f"-c lock_timeout={int(lock_timeout_seconds * 1000)} "
        f"-c idle_in_transaction_session_timeout={int(idle_in_transaction_timeout_seconds * 1000)}"
    )
    return create_engine(
        database_url,
        pool_pre_ping=True,  # discard dead connections instead of handing them out
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,  # how long a caller waits for a free connection
        connect_args={"connect_timeout": 3, "options": options},
    )


def engine_from_settings(settings: Settings) -> Engine:
    return make_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout_seconds,
        statement_timeout_seconds=settings.db_statement_timeout_seconds,
        lock_timeout_seconds=settings.db_lock_timeout_seconds,
        idle_in_transaction_timeout_seconds=settings.db_idle_in_transaction_timeout_seconds,
    )


def database_is_reachable(engine: Engine) -> bool:
    """Return True if a trivial query succeeds within a short timeout."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except SQLAlchemyError:
        return False
    return True
