"""A7 official tick disk-floor contract."""

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
    monkeypatch.setattr(scheduler, "_last_disk_floor_log_at", None, raising=False)
    monkeypatch.delenv("HERMES_CRON_MIN_FREE_GIB", raising=False)
    yield


def test_default_floor_is_ten_gib():
    assert scheduler._get_cron_min_free_bytes() == int(10.0 * (1024 ** 3))


def test_env_override_floor(monkeypatch):
    monkeypatch.setenv("HERMES_CRON_MIN_FREE_GIB", "2.5")
    assert scheduler._get_cron_min_free_bytes() == int(2.5 * (1024 ** 3))


def test_env_zero_disables_guard(monkeypatch):
    monkeypatch.setenv("HERMES_CRON_MIN_FREE_GIB", "0")
    monkeypatch.setattr(scheduler.shutil, "disk_usage", lambda _p: _Usage(free=1))
    assert scheduler._check_cron_disk_floor() is None


def test_env_garbage_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("HERMES_CRON_MIN_FREE_GIB", "not-a-number")
    assert scheduler._get_cron_min_free_bytes() == int(10.0 * (1024 ** 3))


def test_above_floor_returns_none(monkeypatch):
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: scheduler.Path("/x"))
    monkeypatch.setattr(scheduler.shutil, "disk_usage", lambda _p: _Usage(free=71 * (1024 ** 3)))
    assert scheduler._check_cron_disk_floor() is None


def test_below_floor_returns_typed_receipt(monkeypatch):
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: scheduler.Path("/x"))
    free = 1 * (1024 ** 3)
    monkeypatch.setattr(scheduler.shutil, "disk_usage", lambda _p: _Usage(free=free))
    receipt = scheduler._check_cron_disk_floor()
    assert receipt == {
        "reason": "LOCAL_DISK_BELOW_FLOOR",
        "free_bytes": free,
        "floor_bytes": int(10.0 * (1024 ** 3)),
        "path": "/x",
    }


def test_measurement_error_does_not_park(monkeypatch):
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: scheduler.Path("/x"))

    def _boom(_p):
        raise OSError("cannot stat volume")

    monkeypatch.setattr(scheduler.shutil, "disk_usage", _boom)
    assert scheduler._check_cron_disk_floor() is None


def test_log_throttled_to_one_per_window(monkeypatch):
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: scheduler.Path("/x"))
    monkeypatch.setattr(scheduler.shutil, "disk_usage", lambda _p: _Usage(free=1 * (1024 ** 3)))
    calls = []
    monkeypatch.setattr(scheduler.logger, "error", lambda *a, **k: calls.append(a))
    assert scheduler._check_cron_disk_floor(now=1000.0) is not None
    assert scheduler._check_cron_disk_floor(now=1100.0) is not None
    assert scheduler._check_cron_disk_floor(now=1250.0) is not None
    assert len(calls) == 1
    assert scheduler._check_cron_disk_floor(now=1301.0) is not None
    assert len(calls) == 2
