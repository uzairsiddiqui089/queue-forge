"""Create an API client and print its key ONCE.

Usage: uv run python scripts/create_client.py <name> [--admin]

The key is shown only here; the database keeps just its hash.
"""

import argparse

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from queueforge.config import get_settings
from queueforge.db.engine import make_engine
from queueforge.db.models import ApiClient
from queueforge.services.auth import generate_api_key, hash_api_key


def main() -> None:
    parser = argparse.ArgumentParser(description="Create an API client")
    parser.add_argument("name")
    parser.add_argument("--admin", action="store_true")
    args = parser.parse_args()

    key = generate_api_key()
    engine = make_engine(get_settings().database_url)
    with Session(engine) as session:
        session.add(
            ApiClient(
                name=args.name,
                key_hash=hash_api_key(key),
                role="admin" if args.admin else "client",
            )
        )
        try:
            session.commit()
        except IntegrityError:
            message = f"A client named '{args.name}' already exists. Pick another name."
            raise SystemExit(message) from None
    print(f"Client '{args.name}' created.")
    print(f"API key (save it now, it cannot be shown again): {key}")


if __name__ == "__main__":
    main()
