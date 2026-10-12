from queueforge.domain.states import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATUSES,
    JobStatus,
    can_transition,
)


def test_legal_transitions() -> None:
    assert can_transition(JobStatus.QUEUED, JobStatus.RUNNING)
    assert can_transition(JobStatus.RUNNING, JobStatus.SUCCEEDED)
    assert can_transition(JobStatus.RUNNING, JobStatus.FAILED)
    assert can_transition(JobStatus.RUNNING, JobStatus.QUEUED)  # retry
    assert can_transition(JobStatus.RUNNING, JobStatus.DEAD_LETTERED)


def test_illegal_transitions() -> None:
    assert not can_transition(JobStatus.QUEUED, JobStatus.SUCCEEDED)  # must run first
    assert not can_transition(JobStatus.QUEUED, JobStatus.DEAD_LETTERED)
    assert not can_transition(JobStatus.SUCCEEDED, JobStatus.QUEUED)


def test_terminal_states_have_no_exits() -> None:
    assert {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD_LETTERED} == TERMINAL_STATUSES
    for status in TERMINAL_STATUSES:
        assert ALLOWED_TRANSITIONS[status] == frozenset()


def test_every_status_has_an_entry() -> None:
    assert set(ALLOWED_TRANSITIONS) == set(JobStatus)
