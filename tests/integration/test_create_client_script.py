"""scripts/create_client.py: creates a client once, and a repeat name is a clear message."""

import os
import subprocess
import sys
from pathlib import Path

from sqlalchemy import Engine, text

ROOT = Path(__file__).resolve().parents[2]


def run_script(*args: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "DATABASE_URL": os.environ["TEST_DATABASE_URL"]}
    return subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "create_client.py"), *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def test_a_client_is_created_and_a_repeated_name_gives_a_short_message(engine: Engine) -> None:
    first = run_script("script-test-client")
    assert first.returncode == 0, first.stderr
    assert "API key" in first.stdout

    again = run_script("script-test-client")
    assert again.returncode != 0
    assert "already exists" in again.stderr
    assert "Traceback" not in again.stderr  # a message, not a stack trace

    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT count(*) FROM api_clients WHERE name = 'script-test-client'")
        ).scalar_one()
    assert count == 1
