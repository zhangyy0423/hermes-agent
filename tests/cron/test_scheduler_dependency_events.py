"""One-hop cron dependency events bind a validated upstream artifact."""

from __future__ import annotations

import copy
import hashlib
from datetime import datetime, timezone

import pytest

import cron.jobs as cron_jobs
import cron.scheduler as scheduler


NOW = datetime(2026, 8, 14, 17, 0, tzinfo=timezone.utc)
UPSTREAM_ID = "upstream-job"
DOWNSTREAM_ID = "downstream-job"
RUN_ID = "upstream-run-1"


def _execution(*, run_id=RUN_ID, status="completed", claimed_at=None):
    return {
        "id": run_id,
        "job_id": UPSTREAM_ID,
        "status": status,
        "claimed_at": claimed_at or "2026-08-14T16:40:00+00:00",
    }


def _jobs(tmp_path):
    upstream = {
        "id": UPSTREAM_ID,
        "name": "upstream",
        "workdir": str(tmp_path),
    }
    downstream = {
        "id": DOWNSTREAM_ID,
        "name": "downstream",
        "enabled": True,
        "state": "scheduled",
        "depends_on": {
            "upstream_job_id": UPSTREAM_ID,
            "mode": "success_artifact",
            "artifact_path": "reports/{business_date}.md",
            "artifact_min_bytes": 10,
            "timezone": "UTC",
            "validator": "sha256_readback_v1",
        },
    }
    return upstream, downstream


def _install_store(monkeypatch, jobs, latest):
    updates = []

    def update_job(job_id, fields):
        target = next(job for job in jobs if job["id"] == job_id)
        target.update(copy.deepcopy(fields))
        updates.append((job_id, copy.deepcopy(fields)))
        return copy.deepcopy(target)

    monkeypatch.setattr(scheduler, "load_jobs", lambda: jobs, raising=False)
    monkeypatch.setattr(scheduler, "update_job", update_job, raising=False)
    monkeypatch.setattr(
        scheduler,
        "latest_execution",
        lambda job_id: copy.deepcopy(latest) if job_id == UPSTREAM_ID else None,
        raising=False,
    )
    monkeypatch.setattr(scheduler, "_hermes_now", lambda: NOW)
    return updates


def _write_artifact(tmp_path, business_date="2026-08-14"):
    path = tmp_path / "reports" / f"{business_date}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = b"validated market pulse artifact\n"
    path.write_bytes(payload)
    return path, payload


def test_success_queues_bound_dependency_event(monkeypatch, tmp_path):
    upstream, downstream = _jobs(tmp_path)
    artifact, payload = _write_artifact(tmp_path)
    execution = _execution()
    updates = _install_store(monkeypatch, [upstream, downstream], execution)

    assert hasattr(scheduler, "_queue_one_hop_dependents")
    assert scheduler._queue_one_hop_dependents(upstream, execution) == 1

    assert len(updates) == 1
    job_id, fields = updates[0]
    assert job_id == DOWNSTREAM_ID
    assert fields["next_run_at"] == NOW.isoformat()
    event = fields["dependency_event"]
    assert event["upstream_job_id"] == UPSTREAM_ID
    assert event["upstream_run_id"] == RUN_ID
    assert event["downstream_job_id"] == DOWNSTREAM_ID
    assert event["business_date"] == "2026-08-14"
    assert event["artifact_path"] == str(artifact.resolve())
    assert event["artifact_sha256"] == hashlib.sha256(payload).hexdigest()
    assert event["validator"] == {
        "name": "sha256_readback_v1",
        "status": "passed",
    }
    assert event["state"] == "ready"


def test_duplicate_trigger_is_deduplicated(monkeypatch, tmp_path):
    upstream, downstream = _jobs(tmp_path)
    _write_artifact(tmp_path)
    execution = _execution()
    updates = _install_store(monkeypatch, [upstream, downstream], execution)

    assert scheduler._queue_one_hop_dependents(upstream, execution) == 1
    assert scheduler._queue_one_hop_dependents(upstream, execution) == 0
    assert len(updates) == 1


def test_restart_uses_persisted_event_for_idempotency(monkeypatch, tmp_path):
    upstream, downstream = _jobs(tmp_path)
    _write_artifact(tmp_path)
    execution = _execution()
    first_updates = _install_store(monkeypatch, [upstream, downstream], execution)
    assert scheduler._queue_one_hop_dependents(upstream, execution) == 1
    assert len(first_updates) == 1

    reloaded_downstream = copy.deepcopy(downstream)
    second_updates = _install_store(
        monkeypatch,
        [copy.deepcopy(upstream), reloaded_downstream],
        execution,
    )
    assert scheduler._queue_one_hop_dependents(upstream, execution) == 0
    assert second_updates == []


def test_dependency_event_is_consumed_once_and_persisted(monkeypatch, tmp_path):
    _, downstream = _jobs(tmp_path)
    downstream["dependency_event"] = {
        "id": "event-1",
        "state": "ready",
    }
    monkeypatch.setattr(cron_jobs, "_hermes_now", lambda: NOW)

    with cron_jobs.use_cron_store(tmp_path):
        cron_jobs.save_jobs([downstream])
        consumed = cron_jobs.consume_dependency_event(
            DOWNSTREAM_ID, "event-1", "downstream-run-1"
        )
        assert consumed == {
            "id": "event-1",
            "state": "consumed",
            "consumed_by_execution_id": "downstream-run-1",
            "consumed_at": NOW.isoformat(),
        }
        assert (
            cron_jobs.consume_dependency_event(
                DOWNSTREAM_ID, "event-1", "downstream-run-2"
            )
            is None
        )
        persisted = cron_jobs.load_jobs()[0]["dependency_event"]

    assert persisted == consumed


@pytest.mark.parametrize(
    ("execution", "latest"),
    [
        (
            _execution(claimed_at="2026-08-13T16:40:00+00:00"),
            _execution(claimed_at="2026-08-13T16:40:00+00:00"),
        ),
        (_execution(), _execution(run_id="newer-run")),
        (_execution(status="failed"), _execution(status="failed")),
    ],
    ids=["old-business-date", "old-run", "failed-run"],
)
def test_invalid_upstream_execution_never_queues(
    monkeypatch, tmp_path, execution, latest
):
    upstream, downstream = _jobs(tmp_path)
    _write_artifact(tmp_path)
    updates = _install_store(monkeypatch, [upstream, downstream], latest)

    assert scheduler._queue_one_hop_dependents(upstream, execution) == 0
    assert updates == []
    assert "dependency_event" not in downstream


def test_missing_artifact_never_queues(monkeypatch, tmp_path):
    upstream, downstream = _jobs(tmp_path)
    execution = _execution()
    updates = _install_store(monkeypatch, [upstream, downstream], execution)

    assert scheduler._queue_one_hop_dependents(upstream, execution) == 0
    assert updates == []


def test_downstream_event_revalidates_bound_artifact(monkeypatch, tmp_path):
    upstream, downstream = _jobs(tmp_path)
    _write_artifact(tmp_path)
    execution = _execution()
    _install_store(monkeypatch, [upstream, downstream], execution)
    assert scheduler._queue_one_hop_dependents(upstream, execution) == 1

    assert scheduler._dependency_event_error(downstream) is None
    (tmp_path / "reports" / "2026-08-14.md").write_text(
        "changed after validation",
        encoding="utf-8",
    )
    assert "artifact SHA" in scheduler._dependency_event_error(downstream)


def test_run_one_job_queues_dependency_only_after_durable_success(monkeypatch):
    events = []
    terminal = _execution()
    monkeypatch.setattr(scheduler, "claim_dispatch", lambda _job_id: True)
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda execution_id: events.append(("running", execution_id)),
    )
    monkeypatch.setattr(
        scheduler,
        "run_job",
        lambda job, *, defer_agent_teardown=None: (
            True,
            "output",
            "final response",
            None,
        ),
    )
    monkeypatch.setattr(scheduler, "save_job_output", lambda *_args: None)
    monkeypatch.setattr(scheduler, "_deliver_result", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scheduler, "mark_job_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda execution_id, **kwargs: events.append(
            ("finish", execution_id, kwargs)
        ) or terminal,
    )
    monkeypatch.setattr(
        scheduler,
        "_queue_one_hop_dependents",
        lambda job, execution: events.append(
            ("queue", job["id"], execution["id"])
        ) or 1,
        raising=False,
    )

    assert scheduler.run_one_job(
        {"id": UPSTREAM_ID, "execution_id": RUN_ID}
    ) is True
    assert [event[0] for event in events] == ["running", "finish", "queue"]


def test_run_one_job_rejects_missing_dependency_event_before_agent(
    monkeypatch, tmp_path
):
    events = []
    job = _jobs(tmp_path)[1]
    job["execution_id"] = "downstream-run-1"

    monkeypatch.setattr(scheduler, "claim_dispatch", lambda _job_id: True)
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda execution_id: events.append(("running", execution_id)),
    )
    monkeypatch.setattr(
        scheduler,
        "run_job",
        lambda *_args, **_kwargs: pytest.fail(
            "dependent job must not start without a validated event"
        ),
    )
    monkeypatch.setattr(
        scheduler,
        "mark_job_run",
        lambda job_id, success, error, **_kwargs: events.append(
            ("mark", job_id, success, error)
        ),
    )
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda execution_id, **kwargs: events.append(
            ("finish", execution_id, kwargs)
        ),
    )

    assert scheduler.run_one_job(job) is False
    assert [event[0] for event in events] == ["running", "mark", "finish"]
    assert events[1][2] is False
    assert "dependency event is missing" in events[1][3]
    assert events[2][2]["success"] is False


def test_run_one_job_consumes_dependency_before_agent(monkeypatch, tmp_path):
    events = []
    job = _jobs(tmp_path)[1]
    job["execution_id"] = "downstream-run-2"
    job["dependency_event"] = {"id": "event-2", "state": "ready"}

    monkeypatch.setattr(scheduler, "claim_dispatch", lambda _job_id: True)
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda execution_id: events.append(("running", execution_id)),
    )
    monkeypatch.setattr(scheduler, "_dependency_event_error", lambda _job: None)
    monkeypatch.setattr(
        scheduler,
        "consume_dependency_event",
        lambda job_id, event_id, execution_id: events.append(
            ("consume", job_id, event_id, execution_id)
        )
        or {
            "id": event_id,
            "state": "consumed",
            "consumed_by_execution_id": execution_id,
        },
    )
    monkeypatch.setattr(
        scheduler,
        "run_job",
        lambda job, *, defer_agent_teardown=None: events.append(
            ("run", job["dependency_event"]["state"])
        )
        or (True, "output", "final response", None),
    )
    monkeypatch.setattr(scheduler, "save_job_output", lambda *_args: None)
    monkeypatch.setattr(scheduler, "_deliver_result", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scheduler, "mark_job_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda execution_id, **_kwargs: {
            "id": execution_id,
            "job_id": DOWNSTREAM_ID,
            "status": "completed",
        },
    )

    assert scheduler.run_one_job(job) is True
    assert events[:3] == [
        ("running", "downstream-run-2"),
        ("consume", DOWNSTREAM_ID, "event-2", "downstream-run-2"),
        ("run", "consumed"),
    ]
