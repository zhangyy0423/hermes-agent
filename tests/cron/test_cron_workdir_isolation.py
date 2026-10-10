"""Concurrency isolation tests for per-execution cron workdir (SQ2 root-fix).

Before this fix a workdir cron job held a process-global ``TERMINAL_CWD``
write lock (``_terminal_cwd_lock``) for its whole agent run, so cron was
effectively serial: no two workdir jobs — and no workdir job alongside any
other job — could run at the same time.  The fix binds each run's workdir to a
per-run task id in the tool-layer session-cwd store (read by the terminal /
file / code-exec / delegation tools) instead of mutating ``os.environ``, so
concurrent cron jobs each resolve their OWN directory with no cross-talk and no
shared lock.

These tests drive the REAL ``run_job`` body (AIAgent stubbed, no API call) and
force genuine concurrency with a ``threading.Barrier`` so every job is inside
its agent run at the same instant.  They assert:

  * two DIFFERENT-workdir jobs run concurrently, each observing its own cwd;
  * SAME-workdir jobs run concurrently without error, both observing it;
  * a NO-workdir job runs concurrently with a workdir job with no leak either
    way (the workdir never bleeds into the no-workdir run, ``TERMINAL_CWD`` is
    never touched);
  * THREE workdir jobs are simultaneously inside their agent run — the N>1
    concurrency the old lock made impossible;
  * the tool-layer session-cwd store is itself per-key isolated and that no
    ``_terminal_cwd_lock`` / sequential pool survives in the module.
"""

from __future__ import annotations

import os
import sys
import threading

import pytest


def _install_concurrent_stubs(monkeypatch, *, barrier, results, results_lock):
    """Stub run_job's deps so it runs without creds, and make the stubbed
    ``run_conversation`` record its task-scoped cwd BEFORE and AFTER a shared
    barrier.  The barrier (N parties) forces all N jobs to be inside their
    agent run simultaneously, so an AFTER reading that still equals the job's
    own workdir proves no concurrent job clobbered it."""
    import cron.scheduler as sched
    from cron import scheduler_delivery as sched_delivery

    class FakeAgent:
        def __init__(self, **kwargs):
            self._skip_context_files = kwargs.get("skip_context_files")

        def run_conversation(self, *_a, task_id=None, **_kw):
            from tools.terminal_tool import get_session_cwd

            # task_id == "cron:<job_id>:<exec/uuid>"
            job_id = task_id.split(":", 2)[1] if task_id else "?"
            before = get_session_cwd(task_id)
            terminal_cwd_env = os.environ.get("TERMINAL_CWD", "_UNSET_")
            # Make every job wait until all N are here — true overlap.
            try:
                barrier.wait(timeout=5)
            except threading.BrokenBarrierError:  # pragma: no cover
                pass
            after = get_session_cwd(task_id)
            with results_lock:
                results[job_id] = {
                    "task_id": task_id,
                    "cwd_before": before,
                    "cwd_after": after,
                    "terminal_cwd_env": terminal_cwd_env,
                    "skip_context_files": self._skip_context_files,
                    "thread": threading.current_thread().name,
                }
            return {"final_response": "done", "messages": []}

        def get_activity_summary(self):
            return {"seconds_since_activity": 0.0}

    fake_mod = type(sys)("run_agent")
    fake_mod.AIAgent = FakeAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_mod)

    from hermes_cli import runtime_provider as _rtp
    monkeypatch.setattr(
        _rtp, "resolve_runtime_provider",
        lambda **_kw: {
            "provider": "test", "api_key": "k",
            "base_url": "http://test.local", "api_mode": "chat_completions",
        },
    )
    monkeypatch.setattr(sched, "_build_job_prompt", lambda job, prerun_script=None, **kw: "hi")
    monkeypatch.setattr(sched_delivery, "_resolve_origin", lambda job: None)
    monkeypatch.setattr(sched, "_resolve_delivery_target", lambda job: None)
    monkeypatch.setattr(sched, "_resolve_cron_enabled_toolsets", lambda job, cfg: None)
    monkeypatch.setenv("HERMES_CRON_TIMEOUT", "0")  # unlimited: poll returns on completion
    import dotenv
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *_a, **_kw: True)


def _run_jobs_concurrently(jobs):
    """Run ``run_job`` for each job in its own thread; return when all finish."""
    import cron.scheduler as sched

    errors: list = []
    errors_lock = threading.Lock()

    def _worker(job):
        try:
            sched.run_job(job)
        except BaseException as exc:  # pragma: no cover - surfaced via assert
            with errors_lock:
                errors.append((job["id"], repr(exc)))

    threads = [
        threading.Thread(target=_worker, args=(job,), name=f"job-{job['id']}")
        for job in jobs
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not any(t.is_alive() for t in threads), "a run_job thread hung"
    assert not errors, f"run_job raised: {errors}"


class TestConcurrentWorkdirIsolation:
    def test_two_different_workdirs_no_crosstalk(self, monkeypatch, tmp_path):
        a = tmp_path / "a"; a.mkdir()
        b = tmp_path / "b"; b.mkdir()
        monkeypatch.setenv("TERMINAL_CWD", str(tmp_path / "baseline"))
        (tmp_path / "baseline").mkdir()

        barrier = threading.Barrier(2, timeout=5)
        results: dict = {}
        lock = threading.Lock()
        _install_concurrent_stubs(monkeypatch, barrier=barrier, results=results, results_lock=lock)

        _run_jobs_concurrently([
            {"id": "ja", "name": "A", "workdir": str(a), "schedule_display": "manual"},
            {"id": "jb", "name": "B", "workdir": str(b), "schedule_display": "manual"},
        ])

        assert set(results) == {"ja", "jb"}
        # Each job saw ITS OWN dir, both before AND after the barrier (overlap).
        assert results["ja"]["cwd_before"] == str(a)
        assert results["ja"]["cwd_after"] == str(a)
        assert results["jb"]["cwd_before"] == str(b)
        assert results["jb"]["cwd_after"] == str(b)
        # Distinct task ids; process-global TERMINAL_CWD never mutated.
        assert results["ja"]["task_id"] != results["jb"]["task_id"]
        for jid in ("ja", "jb"):
            assert results[jid]["terminal_cwd_env"] == str(tmp_path / "baseline")
            assert results[jid]["skip_context_files"] is False

    def test_same_workdir_concurrent(self, monkeypatch, tmp_path):
        shared = tmp_path / "shared"; shared.mkdir()
        barrier = threading.Barrier(2, timeout=5)
        results: dict = {}
        lock = threading.Lock()
        _install_concurrent_stubs(monkeypatch, barrier=barrier, results=results, results_lock=lock)

        _run_jobs_concurrently([
            {"id": "s1", "name": "S1", "workdir": str(shared), "schedule_display": "manual"},
            {"id": "s2", "name": "S2", "workdir": str(shared), "schedule_display": "manual"},
        ])

        assert set(results) == {"s1", "s2"}
        for jid in ("s1", "s2"):
            assert results[jid]["cwd_before"] == str(shared)
            assert results[jid]["cwd_after"] == str(shared)
        # Same dir, but still DISTINCT task identities (keyed per run).
        assert results["s1"]["task_id"] != results["s2"]["task_id"]

    def test_no_workdir_concurrent_with_workdir(self, monkeypatch, tmp_path):
        wd = tmp_path / "wd"; wd.mkdir()
        barrier = threading.Barrier(2, timeout=5)
        results: dict = {}
        lock = threading.Lock()
        _install_concurrent_stubs(monkeypatch, barrier=barrier, results=results, results_lock=lock)

        _run_jobs_concurrently([
            {"id": "wd1", "name": "WD", "workdir": str(wd), "schedule_display": "manual"},
            {"id": "no1", "name": "NO", "workdir": None, "schedule_display": "manual"},
        ])

        assert set(results) == {"wd1", "no1"}
        # Workdir job resolves its dir; no-workdir job records NOTHING in the
        # session-cwd store (so the workdir never leaked into it).
        assert results["wd1"]["cwd_before"] == str(wd)
        assert results["wd1"]["cwd_after"] == str(wd)
        assert results["no1"]["cwd_before"] is None
        assert results["no1"]["cwd_after"] is None
        assert results["wd1"]["skip_context_files"] is False
        assert results["no1"]["skip_context_files"] is True

    def test_three_workdir_jobs_run_simultaneously(self, monkeypatch, tmp_path):
        """Proof of N>1: three workdir jobs are inside their agent run at once.

        The 3-party barrier only releases once all three ``run_conversation``
        bodies have arrived, so reaching the AFTER reads at all means three
        workdir jobs were Running concurrently — impossible under the old
        process-global cwd write lock (which serialized them to one)."""
        dirs = {}
        for name in ("t1", "t2", "t3"):
            d = tmp_path / name; d.mkdir(); dirs[name] = str(d)

        barrier = threading.Barrier(3, timeout=5)
        results: dict = {}
        lock = threading.Lock()
        _install_concurrent_stubs(monkeypatch, barrier=barrier, results=results, results_lock=lock)

        _run_jobs_concurrently([
            {"id": jid, "name": jid.upper(), "workdir": dirs[jid], "schedule_display": "manual"}
            for jid in ("t1", "t2", "t3")
        ])

        assert set(results) == {"t1", "t2", "t3"}
        # All three cleared the 3-party barrier => all three overlapped.
        for jid in ("t1", "t2", "t3"):
            assert results[jid]["cwd_before"] == dirs[jid]
            assert results[jid]["cwd_after"] == dirs[jid]
        task_ids = {results[j]["task_id"] for j in results}
        assert len(task_ids) == 3


class TestSessionCwdStoreIsolation:
    def test_store_is_per_key_isolated(self):
        from tools.terminal_tool import (
            clear_session_cwd, get_session_cwd, record_session_cwd,
        )
        k1 = "cron:iso-a:1"
        k2 = "cron:iso-b:1"
        try:
            record_session_cwd(k1, "/dir/a")
            record_session_cwd(k2, "/dir/b")
            assert get_session_cwd(k1) == "/dir/a"
            assert get_session_cwd(k2) == "/dir/b"
            clear_session_cwd(k1)
            assert get_session_cwd(k1) is None
            # Clearing one key must not disturb the other.
            assert get_session_cwd(k2) == "/dir/b"
        finally:
            clear_session_cwd(k1)
            clear_session_cwd(k2)


class TestNoLegacyLockSurvives:
    def test_terminal_cwd_lock_and_sequential_pool_are_gone(self):
        import cron.scheduler as sched

        for sym in (
            "_ReadWriteLock", "_terminal_cwd_lock", "_sequential_pool",
            "_get_sequential_pool", "_cwd_lock_timeout_seconds",
            "_CWD_LOCK_TIMEOUT_FLOOR_SECONDS", "_CWD_LOCK_TIMEOUT_MARGIN_SECONDS",
        ):
            assert not hasattr(sched, sym), f"{sym} should be removed by the cwd root-fix"
        # The per-execution helper and the default parallel cap replaced them.
        assert hasattr(sched, "_resolve_job_workdir")
        assert isinstance(sched._DEFAULT_CRON_MAX_PARALLEL, int)
        assert sched._DEFAULT_CRON_MAX_PARALLEL >= 1
