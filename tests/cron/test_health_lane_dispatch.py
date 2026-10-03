"""A2 health-lane contract for the official scheduler split.

Health-class script jobs must not queue behind a saturated LLM lane.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import Future

import cron.scheduler as scheduler


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


def test_health_classification_contract():
    assert scheduler._is_health_class_job(
        _job("w1", name="backtest_orchestrator_daemon_watchdog")
    ) is True
    assert scheduler._is_health_class_job(
        _job("m1", name="中断原因归并(额度/上游/凭证)")
    ) is True
    assert scheduler._is_health_class_job(
        _job("g1", name="市场数据-收盘门禁")
    ) is True
    assert scheduler._is_health_class_job(
        _job("a1", name="短线猎手-盘前预判", no_agent=False)
    ) is False
    assert scheduler._is_health_class_job(
        _job("a2", name="research-deep", no_agent=False, health_class=True)
    ) is True
    workdir_health = _job("a3", name="health script", health_class=True)
    workdir_health["workdir"] = "/tmp"
    assert scheduler._is_health_class_job(workdir_health) is False
    assert scheduler._is_health_class_job(
        _job("w2", name="watchdog-like", health_class=False)
    ) is False
    assert scheduler._is_health_class_job(_job("s1", name="kline-daily-collect")) is False


def test_health_lane_is_not_starved_by_parallel_pool():
    started = threading.Event()
    release = threading.Event()

    def long_job(_job):
        started.set()
        release.wait(timeout=30)

    parallel = scheduler._get_parallel_pool(1)
    health = scheduler._get_health_lane_pool()
    future = parallel.submit(long_job, {"id": "llm"})
    assert started.wait(timeout=5)
    started_at = time.monotonic()
    assert health.submit(lambda _job: True, {"id": "watchdog"}).result(timeout=5) is True
    assert time.monotonic() - started_at <= 5.0
    release.set()
    future.result(timeout=30)


def test_tick_partition_puts_health_jobs_on_health_lane():
    due_jobs = [
        _job("wd", name="daemon watchdog"),
        _job("merge", name="中断原因归并"),
        _job("llm", name="投资早报-生产与投递主链", no_agent=False),
        _job("seq", name="workdir job", script="", no_agent=False),
    ]
    due_jobs[-1]["workdir"] = "/tmp"
    sequential = [j for j in due_jobs if (j.get("workdir") or "").strip()]
    health = [
        j for j in due_jobs
        if not (j.get("workdir") or "").strip() and scheduler._is_health_class_job(j)
    ]
    parallel = [
        j for j in due_jobs
        if not (j.get("workdir") or "").strip() and not scheduler._is_health_class_job(j)
    ]
    assert [j["id"] for j in sequential] == ["seq"]
    assert [j["id"] for j in health] == ["wd", "merge"]
    assert [j["id"] for j in parallel] == ["llm"]


def test_departed_profile_discards_health_pool(tmp_path):
    scheduler._get_health_lane_pool(tmp_path)
    key = scheduler.hermes_home_key(tmp_path)
    assert key in scheduler._health_lane_pools
    scheduler.discard_parallel_pools({key})
    assert key not in scheduler._health_lane_pools


def test_tick_routes_health_jobs_to_dedicated_pool(monkeypatch):
    import cron.scheduler_tick as scheduler_tick

    health = _job("health", name="daemon watchdog")
    llm = _job("llm", name="long research", no_agent=False)
    seen = []
    health_pool = object()
    parallel_pool = object()

    monkeypatch.setattr(scheduler, "_should_yield_tick_to_fresh_gateway", lambda: None)
    monkeypatch.setattr(scheduler, "_get_lock_paths", lambda: (None, None))
    monkeypatch.setattr(scheduler, "_ensure_cron_dir", lambda _path: None)
    monkeypatch.setattr(scheduler, "_acquire_tick_lock", lambda _path: object())
    monkeypatch.setattr(scheduler, "_release_tick_lock", lambda _fd: None)
    monkeypatch.setattr(scheduler, "_check_cron_disk_floor", lambda: None)
    monkeypatch.setattr(scheduler, "_maybe_reap_dead_owners", lambda: None)
    monkeypatch.setattr(scheduler, "_maybe_run_worktree_maintenance", lambda: None)
    monkeypatch.setattr(scheduler, "_sweep_stale_inflight_for_tick", lambda _jobs: None)
    monkeypatch.setattr(scheduler, "_sweep_mcp_orphans", lambda: None)
    monkeypatch.setattr(scheduler, "get_due_jobs", lambda: [health, llm])
    monkeypatch.setattr(scheduler, "advance_next_runs", lambda _ids: None)
    monkeypatch.setattr(scheduler, "_resolve_max_parallel_workers", lambda: 1)
    monkeypatch.setattr(scheduler, "_get_health_lane_pool", lambda: health_pool)
    monkeypatch.setattr(scheduler, "_get_parallel_pool", lambda _workers: parallel_pool)

    def submit(job, pool, process_job):
        seen.append((job["id"], pool))
        future = Future()
        future.set_result(True)
        return future

    monkeypatch.setattr(scheduler, "_submit_with_guard", submit)
    assert scheduler_tick._tick_admitted(verbose=False, sync=True) == 2
    assert seen == [("health", health_pool), ("llm", parallel_pool)]
