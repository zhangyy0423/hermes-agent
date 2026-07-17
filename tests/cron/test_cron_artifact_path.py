"""Cron 业务产物落盘契约测试。"""

from datetime import datetime
from pathlib import Path
import sys

import pytest

import cron.scheduler as scheduler


def test_write_job_artifact_expands_date_and_writes_under_workdir(tmp_path, monkeypatch):
    monkeypatch.setattr(
        scheduler,
        "_hermes_now",
        lambda: datetime(2026, 7, 17, 19, 15, 0),
    )
    job = {
        "id": "artifact-ok",
        "workdir": str(tmp_path),
        "artifact_path": "output/reports/report_{YYYY-MM-DD}.md",
        "artifact_min_chars": 10,
    }

    target = scheduler._write_job_artifact(job, "0123456789")

    assert target == tmp_path / "output/reports/report_2026-07-17.md"
    assert target.read_text(encoding="utf-8") == "0123456789"


def test_write_job_artifact_rejects_path_escape(tmp_path):
    for artifact_path in ("../escape.md", str(tmp_path.parent / "absolute.md")):
        job = {
            "id": "artifact-escape",
            "workdir": str(tmp_path),
            "artifact_path": artifact_path,
        }

        with pytest.raises(ValueError, match="artifact_path"):
            scheduler._write_job_artifact(job, "valid response")


@pytest.mark.skipif(sys.platform == "win32", reason="symlink creation requires privileges")
def test_write_job_artifact_rejects_symlink_escape(tmp_path):
    outside = tmp_path.parent / "outside"
    outside.mkdir(exist_ok=True)
    (tmp_path / "linked").symlink_to(outside, target_is_directory=True)
    job = {
        "id": "artifact-symlink-escape",
        "workdir": str(tmp_path),
        "artifact_path": "linked/report.md",
    }

    with pytest.raises(ValueError, match="artifact_path"):
        scheduler._write_job_artifact(job, "valid response")


def test_write_job_artifact_rejects_response_shorter_than_contract(tmp_path):
    job = {
        "id": "artifact-short",
        "workdir": str(tmp_path),
        "artifact_path": "report.md",
        "artifact_min_chars": 20,
    }

    with pytest.raises(ValueError, match="artifact_min_chars"):
        scheduler._write_job_artifact(job, "too short")

    assert not (tmp_path / "report.md").exists()


def test_run_one_job_marks_failure_when_artifact_write_fails(monkeypatch):
    marks = []
    delivered = []

    monkeypatch.setattr(scheduler, "claim_dispatch", lambda _job_id: True)
    monkeypatch.setattr(
        scheduler,
        "run_job",
        lambda job, defer_agent_teardown=None: (
            True,
            "cron envelope",
            "model response",
            None,
        ),
    )
    monkeypatch.setattr(scheduler, "save_job_output", lambda *_args: "/tmp/out.md")
    monkeypatch.setattr(
        scheduler,
        "_write_job_artifact",
        lambda *_args: (_ for _ in ()).throw(ValueError("artifact failed")),
    )
    monkeypatch.setattr(
        scheduler,
        "_deliver_result",
        lambda _job, content, **_kwargs: delivered.append(content),
    )
    monkeypatch.setattr(
        scheduler,
        "mark_job_run",
        lambda job_id, success, error=None, **_kwargs: marks.append(
            (job_id, success, error)
        ),
    )

    processed = scheduler.run_one_job(
        {
            "id": "artifact-fail",
            "name": "artifact fail",
            "workdir": "/tmp",
            "artifact_path": "report.md",
        }
    )

    assert processed is True
    assert marks == [("artifact-fail", False, "Business artifact write failed: artifact failed")]
    assert delivered
    assert "artifact" in delivered[0].lower()
    assert "model response" not in delivered[0]


def test_run_one_job_without_artifact_path_keeps_existing_behavior(monkeypatch):
    marks = []

    monkeypatch.setattr(scheduler, "claim_dispatch", lambda _job_id: True)
    monkeypatch.setattr(
        scheduler,
        "run_job",
        lambda job, defer_agent_teardown=None: (True, "out", "response", None),
    )
    monkeypatch.setattr(scheduler, "save_job_output", lambda *_args: "/tmp/out.md")
    monkeypatch.setattr(
        scheduler,
        "_write_job_artifact",
        lambda *_args: pytest.fail("artifact writer must not run without artifact_path"),
    )
    monkeypatch.setattr(scheduler, "_deliver_result", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scheduler,
        "mark_job_run",
        lambda job_id, success, error=None, **_kwargs: marks.append(
            (job_id, success, error)
        ),
    )

    assert scheduler.run_one_job({"id": "legacy", "name": "legacy"}) is True
    assert marks == [("legacy", True, None)]


def test_prerun_script_receives_job_workdir(monkeypatch, tmp_path):
    seen = {}

    def fake_run(script_path, *, workdir=None):
        seen["script_path"] = script_path
        seen["workdir"] = workdir
        return True, "context"

    monkeypatch.setattr(scheduler, "_run_job_script", fake_run)

    result = scheduler._run_job_script_with_claim_heartbeat(
        {
            "id": "workdir-script",
            "script": "context.py",
            "workdir": str(tmp_path),
        },
        "context.py",
    )

    assert result == (True, "context")
    assert seen == {"script_path": "context.py", "workdir": str(tmp_path)}


def test_build_job_prompt_can_fail_closed_on_script_error(monkeypatch):
    monkeypatch.setattr(
        scheduler,
        "_run_job_script",
        lambda *_args, **_kwargs: (False, "missing main report"),
    )
    job = {
        "id": "script-required",
        "prompt": "analyze",
        "script": "context.py",
        "script_fail_closed": True,
    }

    with pytest.raises(scheduler.CronPrerequisiteFailed, match="missing main report"):
        scheduler._build_job_prompt(job)
