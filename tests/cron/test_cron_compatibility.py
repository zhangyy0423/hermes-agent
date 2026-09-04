"""Regression contracts for Polaris cron extensions on upstream Hermes."""

import copy
import hashlib
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


def test_business_artifact_is_written_atomically_under_workdir(tmp_path):
    import cron.scheduler as scheduler

    job = {
        "id": "artifact-job",
        "workdir": str(tmp_path),
        "artifact_path": "reports/{YYYY-MM-DD}.md",
        "artifact_min_chars": 5,
    }

    target = scheduler._write_job_artifact(job, "fresh report")

    assert target == tmp_path / "reports" / f"{scheduler._hermes_now():%Y-%m-%d}.md"
    assert target.read_text() == "fresh report"
    assert not list(target.parent.glob(".*.tmp"))


def test_business_artifact_rejects_path_escape(tmp_path):
    import cron.scheduler as scheduler

    with pytest.raises(ValueError, match="relative to workdir"):
        scheduler._write_job_artifact(
            {
                "workdir": str(tmp_path),
                "artifact_path": "../escape.md",
                "artifact_min_chars": 1,
            },
            "report",
        )


def test_strict_blocked_receipt_bypasses_artifact_minimum(tmp_path):
    import cron.scheduler as scheduler

    response = '{"status":"BLOCKED","reason_code":"UPSTREAM_DEFERRED","decision_eligible":false}'
    target = scheduler._write_job_artifact(
        {
            "id": "blocked-receipt",
            "workdir": str(tmp_path),
            "artifact_path": "receipt.json",
            "artifact_min_chars": 1000,
        },
        response,
    )

    assert target.read_text(encoding="utf-8") == response
    assert scheduler._is_strict_blocked_receipt(response)
    assert not scheduler._is_strict_blocked_receipt(
        '{"status":"BLOCKED","reason_code":"UPSTREAM_DEFERRED",'
        '"decision_eligible":false,"detail":"extra"}'
    )


def test_blocked_receipt_delivery_is_human_readable_without_mutating_receipt():
    import cron.scheduler as scheduler

    response = (
        '{"status":"BLOCKED","reason_code":"NOT_APPLICABLE_CLOSED_DAY",'
        '"decision_eligible":false}'
    )
    rendered = scheduler._render_blocked_receipt_for_delivery(
        response,
        {
            "id": "short-term-open-auction",
            "name": "短线猎手-集合竞价",
            "workdir": "/Users/zhangyuyu03/agents/polaris",
        },
    )

    assert "短线猎手-集合竞价" in rendered
    assert "NOT_APPLICABLE_CLOSED_DAY" in rendered
    assert "无需人工补跑" in rendered
    assert response == (
        '{"status":"BLOCKED","reason_code":"NOT_APPLICABLE_CLOSED_DAY",'
        '"decision_eligible":false}'
    )


def test_monthly_producer_receipt_binds_snapshot_and_staging(tmp_path, monkeypatch):
    import cron.scheduler as scheduler

    now = datetime(2026, 8, 15, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(scheduler, "_hermes_now", lambda: now)
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: tmp_path)
    report_type = "macro-monthly"
    state_dir = tmp_path / "state" / "research-report-inputs" / report_type
    staging = state_dir / "staging" / "report.md"
    staging.parent.mkdir(parents=True)
    staging.write_text("# monthly staging body\n", encoding="utf-8")
    snapshot = {
        "report_type": report_type,
        "issue_date": "2026-08-15",
        "snapshot_sha256": "a" * 64,
        "staging_report_path": str(staging),
    }
    (state_dir / "current.json").write_text(json.dumps(snapshot), encoding="utf-8")

    artifact = json.loads(
        scheduler._monthly_producer_artifact_response(
            {"id": "b9bf16c7d550", "workdir": "/Users/zhangyuyu03/agents/polaris"}
        )
    )

    assert artifact["schema"] == "polaris.monthly-producer-success.v1"
    assert artifact["status"] == "SUCCESS"
    assert artifact["producer_job_id"] == "b9bf16c7d550"
    assert artifact["business_date"] == "2026-08-15"
    assert artifact["staging_report_path"] == str(staging.resolve())
    assert artifact["staging_sha256"] == hashlib.sha256(staging.read_bytes()).hexdigest()


def test_success_upstream_queues_and_revalidates_dependency_event(monkeypatch, tmp_path):
    import cron.scheduler as scheduler

    now = datetime(2026, 8, 14, 17, 0, tzinfo=timezone.utc)
    upstream_id = "upstream-job"
    downstream_id = "downstream-job"
    upstream = {"id": upstream_id, "workdir": str(tmp_path)}
    downstream = {
        "id": downstream_id,
        "enabled": True,
        "state": "scheduled",
        "depends_on": {
            "upstream_job_id": upstream_id,
            "mode": "success_artifact",
            "artifact_path": "reports/{business_date}.md",
            "artifact_min_bytes": 10,
            "timezone": "UTC",
            "validator": "sha256_readback_v1",
        },
    }
    artifact = tmp_path / "reports" / "2026-08-14.md"
    artifact.parent.mkdir()
    artifact.write_text("validated artifact\n", encoding="utf-8")
    execution = {
        "id": "upstream-run-1",
        "job_id": upstream_id,
        "status": "completed",
        "claimed_at": "2026-08-14T16:40:00+00:00",
    }
    updates = []

    def update_job(job_id, fields):
        assert job_id == downstream_id
        downstream.update(copy.deepcopy(fields))
        updates.append(copy.deepcopy(fields))
        return copy.deepcopy(downstream)

    monkeypatch.setattr(scheduler, "_hermes_now", lambda: now)
    monkeypatch.setattr(scheduler, "load_jobs", lambda: [upstream, downstream])
    monkeypatch.setattr(scheduler, "update_job", update_job)
    monkeypatch.setattr(scheduler, "latest_completed_execution", lambda _job_id: execution)

    assert scheduler._queue_one_hop_dependents(upstream, execution) == 1
    assert updates[0]["dependency_event"]["state"] == "ready"
    assert scheduler._dependency_event_error(downstream) is None

    artifact.write_text("changed artifact\n", encoding="utf-8")
    assert "artifact SHA" in scheduler._dependency_event_error(downstream)


def test_blocked_receipt_is_persisted_without_queueing_downstream(monkeypatch, tmp_path):
    import cron.scheduler as scheduler

    blocked = (
        '{"status":"BLOCKED","reason_code":"UPSTREAM_DEFERRED",'
        '"decision_eligible":false}'
    )
    queued = []
    monkeypatch.setattr(scheduler, "create_execution", lambda *_a, **_k: {"id": "run-1"})
    monkeypatch.setattr(scheduler, "claim_dispatch", lambda _job_id: True)
    monkeypatch.setattr(scheduler, "mark_execution_running", lambda _execution_id: None)
    monkeypatch.setattr(
        scheduler,
        "run_job",
        lambda *_a, **_k: (True, "output", blocked, None),
    )
    monkeypatch.setattr(scheduler, "save_job_output", lambda *_a, **_k: tmp_path / "output")
    monkeypatch.setattr(scheduler, "_deliver_result", lambda *_a, **_k: None)
    monkeypatch.setattr(scheduler, "mark_job_run", lambda *_a, **_k: None)
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda *_a, **_k: {
            "id": "run-1",
            "job_id": "producer",
            "status": "completed",
        },
    )
    monkeypatch.setattr(
        scheduler,
        "_queue_one_hop_dependents",
        lambda *_a, **_k: queued.append(True),
    )

    assert scheduler._run_one_job_body(
        {
            "id": "producer",
            "name": "producer",
            "workdir": str(tmp_path),
            "artifact_path": "reports/receipt.json",
            "artifact_min_chars": 1000,
            "deliver": "local",
        }
    )
    assert (tmp_path / "reports" / "receipt.json").read_text(encoding="utf-8") == blocked
    assert queued == []


def test_script_heartbeat_preserves_declared_job_workdir(monkeypatch, tmp_path):
    import cron.scheduler as scheduler

    captured = {}

    def run_script(script_path, *, workdir=None, extra_env=None, cancel_event=None):
        captured.update(
            script_path=script_path,
            workdir=workdir,
            extra_env=extra_env,
            cancel_event=cancel_event,
        )
        return True, "context"

    monkeypatch.setattr(scheduler, "_run_job_script", run_script)

    result = scheduler._run_job_script_with_claim_heartbeat(
        {"id": "workdir-script", "workdir": str(tmp_path)},
        "context.py",
    )

    assert result == (True, "context")
    assert captured["workdir"] == str(tmp_path)


def test_dependent_no_agent_receives_consumed_event_binding(monkeypatch, tmp_path):
    import cron.scheduler as scheduler

    artifact = tmp_path / "artifact.json"
    artifact.write_text("validated artifact\n", encoding="utf-8")
    captured = {}
    job = {
        "id": "monthly-finalizer",
        "name": "monthly-finalizer",
        "no_agent": True,
        "script": "finalize.sh",
        "workdir": str(tmp_path),
        "depends_on": {"upstream_job_id": "monthly-producer"},
        "dependency_event": {
            "id": "e" * 64,
            "state": "consumed",
            "upstream_job_id": "monthly-producer",
            "upstream_run_id": "producer-run-1",
            "business_date": "2026-08-15",
            "artifact_path": str(artifact.resolve()),
            "artifact_sha256": "a" * 64,
        },
    }

    def run_script(job, script_path, **kwargs):
        captured.update(job=job, script_path=script_path, **kwargs)
        return True, '{"status":"CONSUMED"}'

    monkeypatch.setattr(scheduler, "_run_job_script_with_claim_heartbeat", run_script)

    success, _doc, _response, error = scheduler.run_job(job)

    assert success is True
    assert error is None
    assert captured["workdir"] == str(tmp_path)
    assert captured["extra_env"] == {
        "HERMES_CRON_DEPENDENCY_EVENT_ID": "e" * 64,
        "HERMES_CRON_DEPENDENCY_UPSTREAM_JOB_ID": "monthly-producer",
        "HERMES_CRON_DEPENDENCY_UPSTREAM_RUN_ID": "producer-run-1",
        "HERMES_CRON_DEPENDENCY_BUSINESS_DATE": "2026-08-15",
        "HERMES_CRON_DEPENDENCY_ARTIFACT_PATH": str(artifact.resolve()),
        "HERMES_CRON_DEPENDENCY_ARTIFACT_SHA256": "a" * 64,
    }


def test_dependency_event_is_consumed_once(tmp_path, monkeypatch):
    from cron import jobs

    monkeypatch.setattr(jobs, "CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr(jobs, "JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", tmp_path / "cron" / "output")
    jobs.save_jobs([
        {
            "id": "downstream",
            "enabled": True,
            "state": "scheduled",
            "dependency_event": {"id": "event-1", "state": "ready"},
        }
    ])

    consumed = jobs.consume_dependency_event("downstream", "event-1", "run-1")

    assert consumed["state"] == "consumed"
    assert consumed["consumed_by_execution_id"] == "run-1"
    assert jobs.consume_dependency_event("downstream", "event-1", "run-2") is None


def test_skill_requirements_classify_all_attached_skills():
    from cron.jobs import normalize_skill_requirements

    assert normalize_skill_requirements(
        {"required": ["polaris-cio"], "optional": ["news"]},
        ["polaris-cio", "news"],
    ) == {"required": ["polaris-cio"], "optional": ["news"]}

    with pytest.raises(ValueError, match="exactly classify"):
        normalize_skill_requirements(
            {"required": ["polaris-cio"], "optional": []},
            ["polaris-cio", "news"],
        )


def test_dependency_prerequisite_blocks_agent_run(monkeypatch):
    import cron.scheduler as scheduler

    called = []
    marked = []
    monkeypatch.setattr(scheduler, "create_execution", lambda *_a, **_k: {"id": "e1"})
    monkeypatch.setattr(scheduler, "claim_dispatch", lambda _job_id: True)
    monkeypatch.setattr(scheduler, "mark_execution_running", lambda _id: None)
    monkeypatch.setattr(scheduler, "run_job", lambda *_a, **_k: called.append(True))
    monkeypatch.setattr(scheduler, "mark_job_run", lambda *a, **k: marked.append((a, k)))
    monkeypatch.setattr(scheduler, "finish_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(scheduler, "_deliver_result", lambda *_a, **_k: None)

    result = scheduler._run_one_job_body(
        {
            "id": "downstream",
            "name": "downstream",
            "deliver": "local",
            "depends_on": {"upstream_job_id": "upstream"},
        }
    )

    assert result is False
    assert called == []
    assert marked and marked[0][0][2].startswith("Cron dependency prerequisite failed")


def test_required_skill_missing_blocks_prompt(monkeypatch):
    import cron.scheduler as scheduler

    monkeypatch.setattr(
        "tools.skills_tool.skill_view",
        lambda _name: json.dumps({"success": False, "error": "missing"}),
    )

    with pytest.raises(scheduler.CronPrerequisiteFailed, match="required skill"):
        scheduler._build_job_prompt(
            {
                "id": "skill-job",
                "prompt": "run",
                "skills": ["required-skill"],
                "skill_requirements": {
                    "required": ["required-skill"],
                    "optional": [],
                },
            }
        )


class TestParsePrecheckHardBlock:
    """Unit tests for _parse_precheck_hard_block — pure function, no side effects.

    P19 (2026-09-04): Polaris's short-term precheck scripts render a strict
    BLOCKED receipt (POLARIS_SHORT_TERM_BLOCKED_RECEIPT_V1) as their last
    stdout line on engine/upstream failure and still exit 0. Detecting that
    line here — the same way ``_parse_wake_gate`` detects ``wakeAgent`` —
    lets ``run_job`` short-circuit before any LLM call, so the receipt no
    longer depends on the model transcribing it verbatim.
    """

    def test_no_output_returns_none(self):
        import cron.scheduler as scheduler

        assert scheduler._parse_precheck_hard_block("") is None
        assert scheduler._parse_precheck_hard_block(None) is None

    def test_strict_receipt_as_last_line_is_detected(self):
        import cron.scheduler as scheduler

        stdout = (
            "TRADING_DAY\n\n"
            '{"status":"BLOCKED","reason_code":"SHORT_TERM_ENGINE_FAILED",'
            '"decision_eligible":false}'
        )
        assert scheduler._parse_precheck_hard_block(stdout) == (
            '{"status":"BLOCKED","reason_code":"SHORT_TERM_ENGINE_FAILED",'
            '"decision_eligible":false}'
        )

    def test_trailing_blank_lines_after_receipt_are_ignored(self):
        import cron.scheduler as scheduler

        stdout = (
            '{"status":"BLOCKED","reason_code":"SHORT_TERM_ENGINE_FAILED",'
            '"decision_eligible":false}\n\n   \n'
        )
        assert scheduler._parse_precheck_hard_block(stdout) == (
            '{"status":"BLOCKED","reason_code":"SHORT_TERM_ENGINE_FAILED",'
            '"decision_eligible":false}'
        )

    def test_success_output_returns_none(self):
        """The signal-injection success path must be untouched — no receipt
        on the last line means run_job proceeds to the ordinary LLM call."""
        import cron.scheduler as scheduler

        stdout = (
            "TRADING_DAY\n\n"
            "【signals_0925.json 已注入 — 直接分析，禁止再调 terminal/read_file】\n"
            "数据引擎耗时 12.3s，候选信号 2 只\n"
            '{"round": "0925", "candidates": []}'
        )
        assert scheduler._parse_precheck_hard_block(stdout) is None

    def test_malformed_receipt_is_not_treated_as_hard_block(self):
        """An extra key or wrong types must NOT be accepted — this reuses
        ``_is_strict_blocked_receipt``'s exact byte-for-byte contract."""
        import cron.scheduler as scheduler

        malformed = (
            '{"status":"BLOCKED","reason_code":"X","decision_eligible":false,'
            '"extra":"y"}'
        )
        assert scheduler._parse_precheck_hard_block(malformed) is None

    def test_wake_gate_false_takes_precedence_over_hard_block_check(self):
        """A json object without the receipt's exact key set (e.g. a plain
        wakeAgent gate) is correctly rejected by the receipt parser and lets
        the existing wake-gate short-circuit run unaffected."""
        import cron.scheduler as scheduler

        assert scheduler._parse_precheck_hard_block('{"wakeAgent": false}') is None


class TestRunJobPrecheckHardBlock:
    """Integration tests: run_job short-circuits on a precheck hard-block
    receipt without ever constructing the LLM agent."""

    @pytest.fixture(autouse=True)
    def _stub_runtime_provider(self):
        fake_runtime = {
            "provider": "openrouter",
            "api_mode": "chat_completions",
            "base_url": "https://openrouter.ai/api/v1",
            "api_key": "test-key",
            "source": "stub",
            "requested_provider": None,
        }
        with patch(
            "hermes_cli.runtime_provider.resolve_runtime_provider",
            return_value=fake_runtime,
        ):
            yield

    def _make_job(self, name="short-term-precheck-test", script="pre_check.py"):
        return {
            "id": f"job_{name}",
            "name": name,
            "prompt": "You are the short-term hunter.",
            "schedule": "*/5 * * * *",
            "script": script,
        }

    def test_engine_failed_receipt_short_circuits_without_agent_call(self):
        import cron.scheduler as scheduler

        receipt = (
            '{"status":"BLOCKED","reason_code":"SHORT_TERM_ENGINE_FAILED",'
            '"decision_eligible":false}'
        )
        stdout = f"TRADING_DAY\n\n{receipt}"
        with patch.object(
            scheduler, "_run_job_script", return_value=(True, stdout)
        ), patch("run_agent.AIAgent") as agent_cls:
            success, doc, final, err = scheduler.run_job(self._make_job())

        assert success is True
        assert err is None
        assert final == receipt
        assert receipt in doc
        agent_cls.assert_not_called()

    def test_receipt_from_hard_block_passes_artifact_min_chars_exemption(
        self, tmp_path
    ):
        """End-to-end with the real artifact writer: the machine-authored
        receipt returned by run_job must still satisfy
        _write_job_artifact's strict-receipt exemption, exactly like a
        model-transcribed receipt did before this change."""
        import cron.scheduler as scheduler

        receipt = (
            '{"status":"BLOCKED","reason_code":"SHORT_TERM_ENGINE_FAILED",'
            '"decision_eligible":false}'
        )
        stdout = f"TRADING_DAY\n\n{receipt}"
        job = self._make_job()
        job["workdir"] = str(tmp_path)
        job["artifact_path"] = "reports/{YYYY-MM-DD}_0730.md"
        job["artifact_min_chars"] = 500

        with patch.object(
            scheduler, "_run_job_script", return_value=(True, stdout)
        ), patch("run_agent.AIAgent") as agent_cls:
            success, _doc, final, err = scheduler.run_job(job)

        assert success is True
        assert err is None
        agent_cls.assert_not_called()

        target = scheduler._write_job_artifact(job, final)
        assert target.read_text(encoding="utf-8") == receipt
        assert scheduler._is_strict_blocked_receipt(final)

    def test_success_output_still_runs_the_agent_unchanged(self):
        """The signal-injection success path (no hard-block line) must be
        byte-for-byte unaffected: the agent still runs and the script
        output still lands in the prompt, exactly like before this change."""
        import cron.scheduler as scheduler

        script_output = (
            "TRADING_DAY\n\n"
            "【signals_0925.json 已注入 — 直接分析，禁止再调 terminal/read_file】\n"
            '{"round": "0925", "candidates": []}'
        )
        agent = MagicMock()
        agent.run_conversation = MagicMock(
            return_value={"final_response": "report body", "messages": []}
        )
        with patch.object(
            scheduler, "_run_job_script", return_value=(True, script_output)
        ), patch("run_agent.AIAgent", return_value=agent) as agent_cls:
            success, _doc, final, err = scheduler.run_job(self._make_job())

        agent_cls.assert_called_once()
        call_kwargs = agent.run_conversation.call_args
        prompt_arg = (
            call_kwargs.args[0]
            if call_kwargs.args
            else call_kwargs.kwargs.get("user_message", "")
        )
        assert script_output in prompt_arg
        assert success is True
        assert err is None
        assert final == "report body"
