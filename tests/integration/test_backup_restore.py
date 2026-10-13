"""Backup, restore and verify: the scripts in scripts/backup, run for real against PostgreSQL.

The scripts need PostgreSQL's client tools. They are found either on this machine (CI, Linux)
or, when they are not installed (a Windows laptop), inside the Compose `dbtools` container.
If neither is available the tests are skipped.
"""

import os
import shutil
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine, make_url, text

from tests.integration.test_retention import add_job

ROOT = Path(__file__).resolve().parents[2]
MakeClient = Callable[[str], tuple[uuid.UUID, str]]


class Tools:
    """Runs shell commands where the PostgreSQL client tools live."""

    def __init__(self, mode: str, pg_env: dict[str, str]) -> None:
        self.mode = mode
        self.pg_env = pg_env
        self.scripts = str(ROOT / "scripts" / "backup") if mode == "direct" else "/scripts"

    def run(
        self, command: str, env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        extra = {**self.pg_env, **(env or {})}
        if self.mode == "direct":
            return subprocess.run(
                ["bash", "-c", command],
                env={**os.environ, **extra},
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
            )
        flags = [part for k, v in extra.items() for part in ("-e", f"{k}={v}")]
        return subprocess.run(
            ["docker", "compose", "--profile", "tools", "run", "--rm", "-T", *flags,
             "--entrypoint", "bash", "dbtools", "-c", command],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )  # fmt: skip

    def tool(
        self, name: str, *args: str, env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        return self.run(f"bash {self.scripts}/{name}.sh " + " ".join(args), env)


def detect() -> Tools | None:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        return None
    u = make_url(url)
    pg = {
        "PGUSER": u.username or "",
        "PGPASSWORD": u.password or "",
        "PGDATABASE": u.database or "",
    }
    if all(shutil.which(t) for t in ("bash", "pg_dump", "pg_restore", "psql", "createdb")):
        return Tools(
            "direct", {**pg, "PGHOST": u.host or "localhost", "PGPORT": str(u.port or 5432)}
        )
    if shutil.which("docker"):
        probe = subprocess.run(
            ["docker", "compose", "ps", "-q", "db"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if probe.returncode == 0 and probe.stdout.strip():
            return Tools("docker", pg)
    return None


@pytest.fixture(scope="module")
def tools() -> Tools:
    found = detect()
    if found is None:
        if os.environ.get(
            "REQUIRE_BACKUP_TOOLS"
        ):  # CI: a missing tool must be a failure, not a skip
            pytest.fail("REQUIRE_BACKUP_TOOLS is set but the PostgreSQL client tools are missing")
        pytest.skip("PostgreSQL client tools (or the Compose dbtools container) are not available")
    return found


@pytest.fixture
def backup_dir(tools: Tools, tmp_path: Path) -> Iterator[str]:
    path = (
        str(tmp_path / "backups")
        if tools.mode == "direct"
        else f"/backups/test-{uuid.uuid4().hex[:8]}"
    )
    yield path
    tools.run(f"rm -rf {path}")


@pytest.fixture
def scratch(engine: Engine) -> Iterator[Callable[[], str]]:
    """Hands out scratch database names and drops them all afterwards."""
    names: list[str] = []

    def new() -> str:
        names.append(f"qf_t_{uuid.uuid4().hex[:8]}")
        return names[-1]

    yield new
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for name in names:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))


def rows_in(engine: Engine, database: str, table: str) -> int:
    other = create_engine(engine.url.set(database=database))
    try:
        with other.connect() as conn:
            return int(conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one())
    finally:
        other.dispose()


def seed(engine: Engine, make_client: MakeClient) -> None:
    client = make_client("alice")[0]
    for _ in range(5):
        add_job(engine, client, "succeeded", 1, idempotency_key=True, notification=True)
    add_job(engine, client, "queued", 0, attempt=False)


def take_backup(tools: Tools, directory: str, **env: str) -> str:
    result = tools.tool("backup", env={"BACKUP_DIR": directory, **env})
    assert result.returncode == 0, result.stderr
    line = next(ln for ln in result.stdout.splitlines() if ln.startswith("backup_file="))
    return line.split("=", 1)[1]


def test_a_backup_can_be_verified(
    tools: Tools, engine: Engine, make_client: MakeClient, backup_dir: str
) -> None:
    seed(engine, make_client)
    path = take_backup(tools, backup_dir)

    listing = tools.run(f"ls {backup_dir}").stdout.split()
    assert sorted(f.rsplit(".", 1)[-1] for f in listing) == ["dump", "manifest", "sha256"]

    verdict = tools.tool("verify", path)
    assert verdict.returncode == 0, verdict.stdout + verdict.stderr
    assert "VERIFY OK" in verdict.stdout


def test_a_restore_brings_back_the_same_data_in_a_new_database(
    tools: Tools,
    engine: Engine,
    make_client: MakeClient,
    backup_dir: str,
    scratch: Callable[[], str],
) -> None:
    seed(engine, make_client)
    path = take_backup(tools, backup_dir)
    target = scratch()

    restored = tools.tool("restore", path, "--into", target)
    assert restored.returncode == 0, restored.stderr

    for table in ("jobs", "job_attempts", "idempotency_keys", "notifications", "api_clients"):
        live = rows_in(engine, str(engine.url.database), table)
        assert rows_in(engine, target, table) == live, table
    assert rows_in(engine, target, "jobs") == 6


def test_restore_will_not_overwrite_the_live_database_or_an_existing_one(
    tools: Tools,
    engine: Engine,
    make_client: MakeClient,
    backup_dir: str,
    scratch: Callable[[], str],
) -> None:
    seed(engine, make_client)
    path = take_backup(tools, backup_dir)

    live = tools.tool("restore", path, "--into", str(engine.url.database))
    assert live.returncode != 0 and "live database" in live.stderr

    target = scratch()
    assert tools.tool("restore", path, "--into", target).returncode == 0
    again = tools.tool("restore", path, "--into", target)
    assert again.returncode != 0 and "already exists" in again.stderr


def test_a_damaged_backup_is_detected_and_refused(
    tools: Tools,
    engine: Engine,
    make_client: MakeClient,
    backup_dir: str,
    scratch: Callable[[], str],
) -> None:
    seed(engine, make_client)
    path = take_backup(tools, backup_dir)
    tools.run(f"printf 'X' | dd of={path} bs=1 seek=100 conv=notrunc 2>/dev/null")  # flip one byte

    restored = tools.tool("restore", path, "--into", scratch())
    assert restored.returncode != 0 and "checksum mismatch" in restored.stderr
    assert tools.tool("verify", path).returncode != 0


def test_an_encrypted_backup_needs_the_right_passphrase(
    tools: Tools,
    engine: Engine,
    make_client: MakeClient,
    backup_dir: str,
    scratch: Callable[[], str],
) -> None:
    seed(engine, make_client)
    path = take_backup(tools, backup_dir, BACKUP_PASSPHRASE="correct horse battery staple")

    assert path.endswith(".dump.enc")
    assert tools.run(f"ls {backup_dir}").stdout.count(".dump\n") == 0  # no plain copy left behind
    assert "PGDMP" not in tools.run(f"head -c 5 {path}").stdout  # not a readable dump any more

    ok = tools.tool(
        "restore",
        path,
        "--into",
        scratch(),
        env={"BACKUP_PASSPHRASE": "correct horse battery staple"},
    )
    assert ok.returncode == 0, ok.stderr
    wrong = tools.tool("restore", path, "--into", scratch(), env={"BACKUP_PASSPHRASE": "wrong"})
    assert wrong.returncode != 0
    missing = tools.tool("restore", path, "--into", scratch())
    assert missing.returncode != 0 and "BACKUP_PASSPHRASE" in missing.stderr


def test_only_the_newest_backups_are_kept(
    tools: Tools, engine: Engine, make_client: MakeClient, backup_dir: str
) -> None:
    seed(engine, make_client)
    taken = []
    for _ in range(3):
        taken.append(take_backup(tools, backup_dir, BACKUP_KEEP="2"))
        time.sleep(1.1)  # backup names carry the time to the second

    kept = tools.run(f"ls {backup_dir}").stdout.split()
    dumps = [f for f in kept if f.endswith(".dump")]
    assert len(dumps) == 2
    assert Path(taken[0]).name not in kept  # the oldest went, with its checksum and manifest
    assert (
        Path(taken[0]).name + ".sha256" not in kept
        and Path(taken[0]).name + ".manifest" not in kept
    )
    assert Path(taken[2]).name in kept


def test_a_failed_backup_leaves_nothing_behind(tools: Tools, backup_dir: str) -> None:
    result = tools.tool(
        "backup", env={"BACKUP_DIR": backup_dir, "PGDATABASE": "no_such_database_xyz"}
    )
    assert result.returncode != 0
    assert tools.run(f"ls -A {backup_dir}").stdout.strip() == ""  # no partial file, no manifest


def test_verify_can_actually_fail(
    tools: Tools, engine: Engine, make_client: MakeClient, backup_dir: str
) -> None:
    """A checker that always says OK would be worse than none: prove this one can fail."""
    seed(engine, make_client)
    path = take_backup(tools, backup_dir)

    tools.run(
        "sed -i 's/^rows_before.jobs=.*/rows_before.jobs=99999/; "
        f"s/^rows_after.jobs=.*/rows_after.jobs=99999/' {path}.manifest"
    )
    wrong_counts = tools.tool("verify", path)
    assert wrong_counts.returncode != 0 and "table jobs has" in wrong_counts.stdout

    path = take_backup(tools, backup_dir)
    tools.run(f"sed -i 's/^schema_version=.*/schema_version=0001/' {path}.manifest")
    wrong_schema = tools.tool("verify", path)
    assert wrong_schema.returncode != 0 and "schema version" in wrong_schema.stdout


def test_replacing_a_database_needs_confirmation_and_no_open_connections(
    tools: Tools,
    engine: Engine,
    make_client: MakeClient,
    backup_dir: str,
    scratch: Callable[[], str],
) -> None:
    seed(engine, make_client)
    path = take_backup(tools, backup_dir)
    target = scratch()
    assert tools.tool("restore", path, "--into", target).returncode == 0

    unconfirmed = tools.tool("restore", path, "--into", target, "--replace")
    assert unconfirmed.returncode != 0 and "CONFIRM_REPLACE" in unconfirmed.stderr

    confirm = {"CONFIRM_REPLACE": "yes-replace-the-database"}
    busy = create_engine(engine.url.set(database=target))
    with busy.connect():  # someone is still connected: must refuse
        refused = tools.tool("restore", path, "--into", target, "--replace", env=confirm)
        assert refused.returncode != 0 and "connection(s) are open" in refused.stderr
    busy.dispose()

    replaced = tools.tool("restore", path, "--into", target, "--replace", env=confirm)
    assert replaced.returncode == 0, replaced.stderr
    assert rows_in(engine, target, "jobs") == 6
