from datetime import datetime

from sqlalchemy import DateTime
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    # Always store timezone-aware timestamps (timestamptz in PostgreSQL).
    type_annotation_map = {datetime: DateTime(timezone=True)}
