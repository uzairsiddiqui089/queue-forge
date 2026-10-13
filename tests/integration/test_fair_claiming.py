"""Fair claiming: clients take turns by how many jobs each already has running."""

import uuid
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import Engine, select, text
from sqlalchemy.orm import Session, sessionmaker

from queueforge.db.models import Job
from queueforge.services import jobs as job_service
from queueforge.worker.claims import ClaimedJob, claim_next_job
from queueforge.worker.runner import Worker

MakeClient = Callable[[str], tuple[uuid.UUID, str]]


@pytest.fixture
def sessions(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine, expire_on_commit=False)


def add_jobs(sessions: sessionmaker[Session], client_id: uuid.UUID, n: int) -> list[uuid.UUID]:
    ids = []
    for i in range(n):
        with sessions() as session:
            job, _ = job_service.submit_job(session, client_id, "echo", {"message": str(i)}, None)
            ids.append(job.id)
    return ids


def claim(sessions: sessionmaker[Session], **kwargs: object) -> ClaimedJob | None:
    with sessions() as session:
        return claim_next_job(session, "w", 30, **kwargs)  # type: ignore[arg-type]


def owner_of(sessions: sessionmaker[Session], claimed: ClaimedJob) -> uuid.UUID:
    with sessions() as session:
        client_id = session.scalar(select(Job.client_id).where(Job.id == claimed.id))
        assert client_id is not None
        return client_id


def test_a_small_client_is_not_stuck_behind_a_big_backlog(
    sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    big, small = make_client("big")[0], make_client("small")[0]
    add_jobs(sessions, big, 50)  # the backlog arrives first, so it is older
    add_jobs(sessions, small, 1)

    first, second = claim(sessions), claim(sessions)
    assert first is not None and second is not None
    assert owner_of(sessions, first) == big  # nobody is running anything yet: oldest wins
    assert owner_of(sessions, second) == small  # big now has 1 running, small has 0


def test_without_fair_claiming_the_small_client_waits_for_the_whole_backlog(
    sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    big, small = make_client("big")[0], make_client("small")[0]
    add_jobs(sessions, big, 5)
    add_jobs(sessions, small, 1)
    owners = []
    for _ in range(6):
        claimed = claim(sessions, fair=False)
        assert claimed is not None
        owners.append(owner_of(sessions, claimed))
    assert owners == [big] * 5 + [small]  # the old behaviour: strictly oldest first


def test_clients_get_an_equal_share_of_the_workers(
    sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    clients = [make_client(name)[0] for name in ("a", "b", "c")]
    for client in clients:
        add_jobs(sessions, client, 20)

    running: Counter[uuid.UUID] = Counter()
    for _ in range(12):  # 12 workers each take a job and keep it
        claimed = claim(sessions)
        assert claimed is not None
        running[owner_of(sessions, claimed)] += 1
    assert [running[c] for c in clients] == [4, 4, 4]


def test_a_worker_is_never_left_idle_while_one_client_has_work(
    sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    only = make_client("only")[0]
    add_jobs(sessions, only, 7)
    claimed = [claim(sessions) for _ in range(7)]
    assert all(c is not None for c in claimed)
    assert claim(sessions) is None


def test_jobs_waiting_out_a_retry_delay_do_not_win_a_turn(
    sessions: sessionmaker[Session], make_client: MakeClient, engine: Engine
) -> None:
    ready, delayed = make_client("ready")[0], make_client("delayed")[0]
    add_jobs(sessions, ready, 3)
    (delayed_job,) = add_jobs(sessions, delayed, 1)
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE jobs SET run_at = now() + interval '1 hour' WHERE id = :id"),
            {"id": delayed_job},
        )
    for _ in range(3):
        claimed = claim(sessions)
        assert claimed is not None and owner_of(sessions, claimed) == ready
    assert claim(sessions) is None  # the delayed job is not runnable yet


def test_falls_back_to_other_work_when_the_chosen_clients_jobs_are_locked(
    sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    busy, other = make_client("busy")[0], make_client("other")[0]
    add_jobs(sessions, busy, 2)
    add_jobs(sessions, other, 1)
    first = claim(sessions)  # "busy" is older, so it is served first and now has 1 running
    assert first is not None and owner_of(sessions, first) == busy

    # Another worker is in the middle of claiming "other"'s only job (its row is locked).
    with sessions() as other_worker:
        other_worker.execute(select(Job.id).where(Job.client_id == other).with_for_update()).all()
        # Fair choice would be "other" (0 running), but its job is locked: do not sit idle,
        # take the next job from "busy" instead.
        second = claim(sessions)
        assert second is not None and owner_of(sessions, second) == busy
        other_worker.rollback()


def test_more_active_clients_than_the_comparison_limit_still_all_get_served(
    sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    clients = [make_client(f"c{i}")[0] for i in range(5)]
    for client in clients:
        add_jobs(sessions, client, 3)
    taken: list[uuid.UUID] = []
    while (claimed := claim(sessions, max_clients=2)) is not None:  # compare only 2 at a time
        taken.append(claimed.id)
    assert len(taken) == 15 and len(set(taken)) == 15  # everything ran, nothing twice


def test_competing_workers_with_several_clients_never_double_claim(
    sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    clients = [make_client(name)[0] for name in ("a", "b", "c")]
    for client in clients:
        add_jobs(sessions, client, 40)

    def drain(worker_id: str) -> int:
        worker = Worker(sessions, worker_id)
        done = 0
        while worker.run_once():
            done += 1
        return done

    with ThreadPoolExecutor(max_workers=4) as pool:
        counts = list(pool.map(drain, [f"w{i}" for i in range(4)]))

    assert sum(counts) == 120
    with sessions() as session:
        jobs = list(session.scalars(select(Job)))
    assert len(jobs) == 120
    assert all(j.status == "succeeded" and j.attempts == 1 for j in jobs)
