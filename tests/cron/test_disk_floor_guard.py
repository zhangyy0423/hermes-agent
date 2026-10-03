"""A7 official tick disk-floor contract."""

import importlib
import threading
from unittest.mock import patch

import pytest


scheduler = importlib.import_module("cron.scheduler")


class _Usage:
    def __init__(self, free: int) -> None:
        self.total = 100 * (1024 ** 3)
        self.used = self.total - free
        self.free = free


@pytest.fixture(autouse=True)
def _reset_floor_log_state(monkeypatch):
    monkeypatch.setattr(scheduler, "_last_disk_floor_log_at", {}, raising=False)
    monkeypatch.setattr(scheduler, "load_config", lambda: {})
    yield


def test_default_floor_is_ten_gib():
    assert scheduler._get_cron_min_free_bytes() == int(10.0 * (1024 ** 3))


def test_config_override_floor(monkeypatch):
    monkeypatch.setattr(scheduler, "load_config", lambda: {"cron": {"disk_floor_min_free_gib": 2.5}})
    assert scheduler._get_cron_min_free_bytes() == int(2.5 * (1024 ** 3))


def test_config_zero_disables_guard(monkeypatch):
    monkeypatch.setattr(scheduler, "load_config", lambda: {"cron": {"disk_floor_min_free_gib": 0}})
    monkeypatch.setattr(scheduler.shutil, "disk_usage", lambda _p: _Usage(free=1))
    assert scheduler._check_cron_disk_floor() is None


def test_config_garbage_falls_back_to_default(monkeypatch):
    monkeypatch.setattr(scheduler, "load_config", lambda: {"cron": {"disk_floor_min_free_gib": "not-a-number"}})
    assert scheduler._get_cron_min_free_bytes() == int(10.0 * (1024 ** 3))


@pytest.mark.parametrize("raw", ["nan", "inf", "-inf", "1e309"])
def test_nonfinite_or_overflow_floor_falls_back_to_default(monkeypatch, raw):
    monkeypatch.setattr(scheduler, "load_config", lambda: {"cron": {"disk_floor_min_free_gib": raw}})
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


def test_log_throttle_is_scoped_per_profile_home(monkeypatch):
    homes = iter([scheduler.Path("/home-a"), scheduler.Path("/home-b")])
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: next(homes))
    monkeypatch.setattr(scheduler.shutil, "disk_usage", lambda _p: _Usage(free=1 * (1024 ** 3)))
    calls = []
    monkeypatch.setattr(scheduler.logger, "error", lambda *a, **k: calls.append(a))
    assert scheduler._check_cron_disk_floor(now=1000.0) is not None
    assert scheduler._check_cron_disk_floor(now=1000.0) is not None
    assert len(calls) == 2


def test_low_disk_tick_is_not_recorded_as_success(monkeypatch):
    """A parked tick is alive but unsuccessful, and keeps typed blocker evidence."""
    from cron.scheduler_provider import InProcessCronScheduler
    from cron.scheduler_tick import CronTickResult

    beats = []
    errors = []
    stop = threading.Event()

    def parked_tick(*_args, **_kwargs):
        stop.set()
        return CronTickResult(
            0,
            success=False,
            blocker={
                "reason": "LOCAL_DISK_BELOW_FLOOR",
                "free_bytes": 1,
                "floor_bytes": 10,
                "path": "/tmp/hermes",
            },
        )

    provider = InProcessCronScheduler()
    with patch("cron.scheduler.tick", side_effect=parked_tick), \
         patch("cron.jobs.record_ticker_heartbeat", side_effect=lambda success=False: beats.append(success)), \
         patch("cron.jobs.record_ticker_error", side_effect=lambda detail: errors.append(detail)), \
         patch("cron.jobs.clear_ticker_error"):
        thread = threading.Thread(
            target=provider.start,
            args=(stop,),
            kwargs={"interval": 0},
            daemon=True,
        )
        thread.start()
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert beats and all(value is False for value in beats)
    assert any("LOCAL_DISK_BELOW_FLOOR" in str(detail) for detail in errors)


def test_real_tick_guard_returns_typed_blocker(monkeypatch):
    """The actual scheduler_tick admission path carries the disk blocker."""
    import cron.scheduler_tick as scheduler_tick

    blocker = {
        "reason": "LOCAL_DISK_BELOW_FLOOR",
        "free_bytes": 1,
        "floor_bytes": 10,
        "path": "/tmp/hermes",
    }
    monkeypatch.setattr(scheduler, "_should_yield_tick_to_fresh_gateway", lambda: None)
    monkeypatch.setattr(scheduler, "_get_lock_paths", lambda: (None, None))
    monkeypatch.setattr(scheduler, "_ensure_cron_dir", lambda _path: None)
    monkeypatch.setattr(scheduler, "_acquire_tick_lock", lambda _path: object())
    monkeypatch.setattr(scheduler, "_release_tick_lock", lambda _fd: None)
    monkeypatch.setattr(scheduler, "_check_cron_disk_floor", lambda: blocker)

    result = scheduler_tick._tick_admitted(verbose=False, sync=True)

    assert int(result) == 0
    assert result.success is False
    assert result.blocker == blocker
