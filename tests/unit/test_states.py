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
    assert can_transition(JobStatus.DEAD_LETTERED, JobStatus.QUEUED)  # admin requeue


def test_illegal_transitions() -> None:
    assert not can_transition(JobStatus.QUEUED, JobStatus.SUCCEEDED)  # must run first
    assert not can_transition(JobStatus.QUEUED, JobStatus.DEAD_LETTERED)
    assert not can_transition(JobStatus.SUCCEEDED, JobStatus.QUEUED)
    assert not can_transition(JobStatus.FAILED, JobStatus.QUEUED)
    assert not can_transition(JobStatus.DEAD_LETTERED, JobStatus.RUNNING)  # must be re-queued first


def test_final_states_have_no_exits() -> None:
    assert {JobStatus.SUCCEEDED, JobStatus.FAILED} == TERMINAL_STATUSES
    for status in TERMINAL_STATUSES:
        assert ALLOWED_TRANSITIONS[status] == frozenset()


def test_dead_letter_only_exit_is_requeue() -> None:
    assert ALLOWED_TRANSITIONS[JobStatus.DEAD_LETTERED] == {JobStatus.QUEUED}


def test_every_status_has_an_entry() -> None:
    assert set(ALLOWED_TRANSITIONS) == set(JobStatus)
