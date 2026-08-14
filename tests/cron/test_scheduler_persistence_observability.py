"""Scheduler persistence failures expose a stable, redacted stage receipt."""

import errno
import logging

import pytest

import cron.scheduler as scheduler


@pytest.fixture
def isolated_tick_lock(tmp_path, monkeypatch):
    lock_dir = tmp_path / "cron"
    lock_file = lock_dir / ".tick.lock"
    monkeypatch.setattr(
        scheduler,
        "_get_lock_paths",
        lambda: (lock_dir, lock_file),
    )


def _assert_stage_receipt(caplog, captured_stderr, stage):
    expected = (
        "CRON_SCHEDULER_PERSISTENCE_FAILED "
        f"stage={stage} exception=OSError errno={errno.ENOSPC}"
    )
    assert expected in caplog.text
    assert expected in captured_stderr


def test_persistence_receipt_falls_back_to_stderr_when_logger_fails(
    monkeypatch,
    capsys,
):
    failure = OSError(errno.ENOSPC, "sensitive detail")
    monkeypatch.setattr(
        scheduler.logger,
        "error",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("logger down")),
    )

    scheduler._report_scheduler_persistence_failure("due_scan", failure)

    captured = capsys.readouterr()
    expected = (
        "CRON_SCHEDULER_PERSISTENCE_FAILED "
        f"stage=due_scan exception=OSError errno={errno.ENOSPC}"
    )
    assert expected in captured.err
    assert "sensitive detail" not in captured.err


def test_due_scan_persistence_failure_is_observed_and_reraised(
    isolated_tick_lock,
    monkeypatch,
    caplog,
    capsys,
):
    failure = OSError(errno.ENOSPC, "sensitive provider detail")
    monkeypatch.setattr(
        scheduler,
        "get_due_jobs",
        lambda: (_ for _ in ()).throw(failure),
    )
    caplog.set_level(logging.ERROR, logger=scheduler.__name__)

    with pytest.raises(OSError) as raised:
        scheduler.tick(verbose=False)

    assert raised.value is failure
    captured = capsys.readouterr()
    _assert_stage_receipt(caplog, captured.err, "due_scan")
    assert "sensitive provider detail" not in caplog.text
    assert "sensitive provider detail" not in captured.err


def test_next_run_advance_failure_is_observed_before_dispatch_and_reraised(
    isolated_tick_lock,
    monkeypatch,
    caplog,
    capsys,
):
    failure = OSError(errno.ENOSPC, "sensitive job detail")
    dispatched = []
    monkeypatch.setattr(
        scheduler,
        "get_due_jobs",
        lambda: [{"id": "recurring-job"}],
    )
    monkeypatch.setattr(
        scheduler,
        "advance_next_runs",
        lambda _job_ids: (_ for _ in ()).throw(failure),
    )
    monkeypatch.setattr(
        scheduler,
        "run_one_job",
        lambda job, **_kwargs: dispatched.append(job["id"]),
    )
    caplog.set_level(logging.ERROR, logger=scheduler.__name__)

    with pytest.raises(OSError) as raised:
        scheduler.tick(verbose=False)

    assert raised.value is failure
    assert dispatched == []
    captured = capsys.readouterr()
    _assert_stage_receipt(caplog, captured.err, "next_run_advance")
    assert "sensitive job detail" not in caplog.text
    assert "sensitive job detail" not in captured.err
