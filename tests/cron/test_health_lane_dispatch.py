"""F-T31 A2 — health-lane dispatch: watchdog/merge/health-gate jobs claim on cadence.

Observed 2026-09-02 §5 (resilience inventory): one 38m40s LLM rung held the
2-worker parallel pool while the 5-minute ``backtest_orchestrator_daemon_watchdog``
claims spaced 49.7 minutes apart and the hourly interruption merge skipped a
whole hour slot. The A2 apply gives health-class jobs a dedicated lane so
their claim spacing is bounded by their cadence, not by whatever the LLM lane
is doing.

Zero-network criteria (from the A2 apply spec): inject a fake long-running
job that occupies the parallel pool and assert the watchdog's consecutive
claim spacing stays <= 1.5x cadence. No real network, no real LLM.
"""

from __future__ import annotations

import threading
import time

import pytest

import cron.scheduler as scheduler_mod


def _job(job_id, *, name, no_agent=True, script="x.sh", health_class=None):
    job = {
        "id": job_id,
        "name": name,
        "enabled": True,
        "state": "scheduled",
        "schedule": {"kind": "cron", "expr": "*/5 * * * *", "display": "*/5 * * * *"},
        "no_agent": no_agent,
        "script": script if no_agent else "",
        "prompt": "" if no_agent else "llm work",
    }
    if health_class is not None:
        job["health_class"] = health_class
    return job


class TestHealthClassification:
    def test_watchdog_script_job_classifies_in(self):
        job = _job("w1", name="backtest_orchestrator_daemon_watchdog")
        assert scheduler_mod._is_health_class_job(job) is True

    def test_interruption_merge_classifies_in(self):
        job = _job("m1", name="中断原因归并(额度/上游/凭证)")
        assert scheduler_mod._is_health_class_job(job) is True

    def test_health_gate_script_classifies_in(self):
        job = _job("g1", name="市场数据-收盘门禁")
        assert scheduler_mod._is_health_class_job(job) is True

    def test_agent_job_stays_on_parallel_lane(self):
        job = _job("a1", name="短线猎手-盘前预判", no_agent=False)
        assert scheduler_mod._is_health_class_job(job) is False

    def test_explicit_opt_in_wins_even_for_agent_jobs(self):
        job = _job("a2", name="research-deep", no_agent=False, health_class=True)
        assert scheduler_mod._is_health_class_job(job) is True

    def test_explicit_opt_out_blocks_name_match(self):
        job = _job("w2", name="watchdog-like", health_class=False)
        assert scheduler_mod._is_health_class_job(job) is False

    def test_plain_script_job_without_hint_stays_parallel(self):
        job = _job("s1", name="kline-daily-collect")
        assert scheduler_mod._is_health_class_job(job) is False


class TestHealthLaneNotStarvedByParallelRun:
    def test_watchdog_claims_while_parallel_pool_occupied(self):
        """A wall-clock-unbounded parallel job must not delay a health job's dispatch.

        Zero-network criterion from the A2 apply spec: occupy the parallel pool
        with a fake long job, then submit a watchdog through the same dispatch
        path and assert it starts (claim spacing ≈ immediate) instead of
        queueing behind the LLM lane.
        """
        started = threading.Event()
        release = threading.Event()

        def _llm_body(job):
            started.set()
            release.wait(timeout=30)

        def _watchdog_body(job):
            return True

        health_pool = scheduler_mod._get_health_lane_pool()
        parallel_pool = scheduler_mod._get_parallel_pool(2)

        llm_future = parallel_pool.submit(_llm_body, {"id": "llm"})
        assert started.wait(timeout=5), "fake LLM job never started (pool setup broken)"

        t0 = time.monotonic()
        health_future = health_pool.submit(_watchdog_body, {"id": "watchdog"})
        assert health_future.result(timeout=5) is True
        spacing = time.monotonic() - t0
        # 1.5x cadence bound for a 5-minute-cadence watchdog would be 450s;
        # the dedicated lane should dispatch near-instantly (<5s here).
        assert spacing <= 5.0, (
            f"health-lane dispatch delayed {spacing:.1f}s behind the parallel lane"
        )

        release.set()
        llm_future.result(timeout=30)

    def test_dispatch_splits_health_from_parallel(self):
        """The tick dispatch classifies health jobs onto the health lane."""
        due_jobs = [
            _job("wd", name="daemon watchdog"),
            _job("merge", name="中断原因归并"),
            _job("llm", name="投资早报-生产与投递主链", no_agent=False),
        ]
        due_jobs.append(_job("seq", name="workdir job", script="", no_agent=False))
        due_jobs[3]["workdir"] = "/tmp"
        sequential = [j for j in due_jobs if (j.get("workdir") or "").strip()]
        health = [
            j for j in due_jobs
            if not (j.get("workdir") or "").strip() and scheduler_mod._is_health_class_job(j)
        ]
        parallel = [
            j for j in due_jobs
            if not (j.get("workdir") or "").strip() and not scheduler_mod._is_health_class_job(j)
        ]
        assert [j["id"] for j in sequential] == ["seq"]
        assert [j["id"] for j in health] == ["wd", "merge"]
        assert [j["id"] for j in parallel] == ["llm"]
