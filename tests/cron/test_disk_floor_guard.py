"""Tests for the A7 (F-T31 apply) cron-tick disk-floor guard.

Regression cover for the 2026-09-02 incident: the tick hammered ENOSPC every
~28s for 12.7 minutes while executions.db could not even record its own write
failures. The guard must PARK dispatch (return a typed LOCAL_DISK_BELOW_FLOOR
receipt) when free space is below the floor, log at most once per window, and
never itself block a healthy tick on a measurement error. No real disk, no
network.
"""

import importlib

import pytest

scheduler = importlib.import_module("cron.scheduler")


class _Usage:
    def __init__(self, free: int) -> None:
        self.total = 100 * (1024 ** 3)
        self.used = self.total - free
        self.free = free


@pytest.fixture(autouse=True)
def _reset_floor_log_state(monkeypatch):
    # Isolate the module-level throttle clock between tests.
    monkeypatch.setattr(scheduler, "_last_disk_floor_log_at", None, raising=False)
    monkeypatch.delenv("HERMES_CRON_MIN_FREE_GIB", raising=False)
    yield


def test_default_floor_is_ten_gib(monkeypatch):
    assert scheduler._get_cron_min_free_bytes() == int(10.0 * (1024 ** 3))


def test_env_override_floor(monkeypatch):
    monkeypatch.setenv("HERMES_CRON_MIN_FREE_GIB", "2.5")
    assert scheduler._get_cron_min_free_bytes() == int(2.5 * (1024 ** 3))


def test_env_zero_disables_guard(monkeypatch):
    monkeypatch.setenv("HERMES_CRON_MIN_FREE_GIB", "0")
    # Even with almost no free space, a disabled floor never parks.
    monkeypatch.setattr(scheduler.shutil, "disk_usage", lambda _p: _Usage(free=1))
    assert scheduler._check_cron_disk_floor() is None


def test_env_garbage_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("HERMES_CRON_MIN_FREE_GIB", "not-a-number")
    assert scheduler._get_cron_min_free_bytes() == int(10.0 * (1024 ** 3))


def test_above_floor_returns_none(monkeypatch):
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: scheduler.Path("/x"))
    monkeypatch.setattr(
        scheduler.shutil, "disk_usage", lambda _p: _Usage(free=71 * (1024 ** 3))
    )
    assert scheduler._check_cron_disk_floor() is None


def test_below_floor_returns_typed_receipt(monkeypatch):
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: scheduler.Path("/x"))
    free = 1 * (1024 ** 3)
    monkeypatch.setattr(scheduler.shutil, "disk_usage", lambda _p: _Usage(free=free))
    receipt = scheduler._check_cron_disk_floor()
    assert receipt is not None
    assert receipt["reason"] == "LOCAL_DISK_BELOW_FLOOR"
    assert receipt["free_bytes"] == free
    assert receipt["floor_bytes"] == int(10.0 * (1024 ** 3))
    assert receipt["path"] == "/x"


def test_measurement_error_does_not_park(monkeypatch):
    # A shutil.disk_usage failure must not become the reason a healthy tick stops.
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: scheduler.Path("/x"))

    def _boom(_p):
        raise OSError("cannot stat volume")

    monkeypatch.setattr(scheduler.shutil, "disk_usage", _boom)
    assert scheduler._check_cron_disk_floor() is None


def test_log_throttled_to_one_per_window(monkeypatch):
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: scheduler.Path("/x"))
    monkeypatch.setattr(
        scheduler.shutil, "disk_usage", lambda _p: _Usage(free=1 * (1024 ** 3))
    )
    calls = []
    monkeypatch.setattr(
        scheduler.logger, "error", lambda *a, **k: calls.append(a)
    )
    # Three ticks within one 300s window -> exactly one log line.
    assert scheduler._check_cron_disk_floor(now=1000.0) is not None
    assert scheduler._check_cron_disk_floor(now=1100.0) is not None
    assert scheduler._check_cron_disk_floor(now=1250.0) is not None
    assert len(calls) == 1
    # After the window elapses, it logs again (still parking every tick).
    assert scheduler._check_cron_disk_floor(now=1000.0 + 301.0) is not None
    assert len(calls) == 2
