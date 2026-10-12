"""API key handling. Keys are random, shown once, and stored only as a SHA-256 hash."""

import hashlib
import secrets

from sqlalchemy import select
from sqlalchemy.orm import Session

from queueforge.db.models import ApiClient


def generate_api_key() -> str:
    return "qf_" + secrets.token_urlsafe(32)


def hash_api_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def authenticate(session: Session, raw_key: str) -> ApiClient | None:
    """Return the active client that owns this key, or None.

    We look up by the hash of the key. The key has 256 bits of randomness, so
    comparing hashes in the database leaks nothing an attacker could use.
    """
    stmt = select(ApiClient).where(
        ApiClient.key_hash == hash_api_key(raw_key),
        ApiClient.disabled_at.is_(None),
    )
    return session.scalars(stmt).one_or_none()
