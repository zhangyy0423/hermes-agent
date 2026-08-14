"""Behavior tests for cron job-level iteration budgets."""

from unittest.mock import MagicMock, patch

import pytest


def _run_job_with_agent_result(tmp_path, job, agent_result):
    from cron.scheduler import run_job

    (tmp_path / "config.yaml").write_text(
        "model:\n"
        "  default: test-model\n"
        "agent:\n"
        "  max_turns: 60\n",
        encoding="utf-8",
    )
    fake_db = MagicMock()

    with patch("cron.scheduler._hermes_home", tmp_path), \
         patch("cron.scheduler._resolve_origin", return_value=None), \
         patch("hermes_cli.env_loader.load_hermes_dotenv"), \
         patch("hermes_cli.env_loader.reset_secret_source_cache"), \
         patch("hermes_state.SessionDB", return_value=fake_db), \
         patch(
             "hermes_cli.runtime_provider.resolve_runtime_provider",
             return_value={
                 "api_key": "test-key",
                 "base_url": "https://example.invalid/v1",
                 "provider": "openrouter",
                 "api_mode": "chat_completions",
             },
         ), \
         patch("tools.mcp_tool.discover_mcp_tools", return_value=[]), \
         patch("run_agent.AIAgent") as mock_agent_cls:
        mock_agent = MagicMock()
        mock_agent.run_conversation.return_value = agent_result
        mock_agent_cls.return_value = mock_agent
        result = run_job(job)

    return result, mock_agent_cls


class TestResolveCronMaxIterations:
    def test_job_override_wins_over_global_agent_budget(self):
        from cron import scheduler

        assert hasattr(scheduler, "_resolve_cron_max_iterations")
        assert scheduler._resolve_cron_max_iterations(
            {"max_iterations": 300},
            {"agent": {"max_turns": 60}},
        ) == 300

    def test_missing_job_override_uses_existing_global_resolution(self):
        from cron import scheduler

        assert hasattr(scheduler, "_resolve_cron_max_iterations")
        assert scheduler._resolve_cron_max_iterations(
            {},
            {"agent": {"max_turns": 60}},
        ) == 60

    @pytest.mark.parametrize(
        "invalid_value",
        [None, True, False, "300", 0, -1, 10_001],
    )
    def test_explicit_invalid_job_override_fails_closed(self, invalid_value):
        from cron import scheduler

        assert hasattr(scheduler, "_resolve_cron_max_iterations")
        with pytest.raises(ValueError, match="job.max_iterations"):
            scheduler._resolve_cron_max_iterations(
                {"max_iterations": invalid_value},
                {"agent": {"max_turns": 60}},
            )


def test_run_job_passes_job_iteration_override_to_agent(tmp_path):
    job = {
        "id": "large-report",
        "name": "large report",
        "prompt": "write the bounded report",
        "max_iterations": 300,
    }
    result, mock_agent_cls = _run_job_with_agent_result(
        tmp_path,
        job,
        {"final_response": "ok"},
    )
    success, _, final_response, error = result

    assert success is True
    assert error is None
    assert final_response == "ok"
    assert mock_agent_cls.call_args.kwargs["max_iterations"] == 300


def test_iteration_exhaustion_fallback_is_diagnostic_not_success(tmp_path):
    fallback = "UNPUBLISHED FALLBACK: incomplete monthly draft"
    result, _ = _run_job_with_agent_result(
        tmp_path,
        {
            "id": "large-report",
            "name": "large report",
            "prompt": "write the bounded report",
            "max_iterations": 300,
        },
        {
            "final_response": fallback,
            "completed": False,
            "failed": False,
            "turn_exit_reason": "max_iterations_reached(300/300)",
        },
    )
    success, output, final_response, error = result

    assert success is False
    assert final_response == ""
    assert "max_iterations_reached(300/300)" in error
    assert fallback not in error
    assert fallback in output
    assert "FAILED" in output


def test_iteration_exhaustion_marks_failed_without_delivering_fallback(tmp_path):
    from cron import scheduler

    fallback = "UNPUBLISHED FALLBACK: incomplete monthly draft"
    run_result, _ = _run_job_with_agent_result(
        tmp_path,
        {
            "id": "large-report",
            "name": "large report",
            "prompt": "write the bounded report",
            "max_iterations": 300,
        },
        {
            "final_response": fallback,
            "completed": False,
            "failed": False,
            "turn_exit_reason": "max_iterations_reached(300/300)",
        },
    )
    job = {"id": "large-report", "name": "large report", "deliver": "local"}

    with patch("cron.scheduler._get_hermes_home", return_value=tmp_path), \
         patch("cron.scheduler.create_execution", return_value={"id": "run-1"}), \
         patch("cron.scheduler.claim_dispatch", return_value=True), \
         patch("cron.scheduler.mark_execution_running"), \
         patch("cron.scheduler.finish_execution"), \
         patch("cron.scheduler.run_job", return_value=run_result), \
         patch("cron.scheduler.save_job_output", return_value=tmp_path / "run.md") as save_mock, \
         patch("cron.scheduler._deliver_result", return_value=None) as deliver_mock, \
         patch("cron.scheduler.mark_job_run") as mark_mock, \
         patch("cron.scheduler._is_interrupted", return_value=False), \
         patch("cron.scheduler._consume_interrupted_flag", return_value=False):
        assert scheduler.run_one_job(job) is True

    saved_output = save_mock.call_args.args[1]
    delivered_content = deliver_mock.call_args.args[1]
    assert fallback in saved_output
    assert fallback not in delivered_content
    mark_args = mark_mock.call_args.args
    assert mark_args[0] == "large-report"
    assert mark_args[1] is False
    assert "max_iterations_reached(300/300)" in mark_args[2]
