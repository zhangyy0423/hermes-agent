"""北辰 S5 依赖的 resume / catch-up / next_run_at 语义契约。

为什么要有这个文件
------------------
S5（旅游模式恢复）要一次 resume 56 个在 2026-09-24 暂停、``next_run_at`` 已经
过期约两周的 job。它的执行包建立在下面这组语义之上：

1. ``resume_job()`` **保留**过期的 ``next_run_at``，不悄悄重锚到未来；
2. due 扫描对「迟到远超 grace」的周期 job 按 ``cron.catch_up_missed`` 裁决：
   默认 True → 补跑**一次**并把 ``next_run_at`` 前推（不是把两周的 slot 全补）；
3. ``cron.catch_up_missed`` 的默认值就是 True；
4. grace = 周期的一半，clamp 到 [120s, 7200s]；
5. ``enabled=False`` 的 job 即使 ``next_run_at`` 已到也不会 due ——
   这是执行包「先预置 next_run_at、再官方 resume」两步法的安全前提。

官方 v2026.9.24 把 resume + due 扫描 + catch-up 路径上**每一处** wall-clock
比较和加减都换成了 UTC instant helper（``_instant_at_or_before`` /
``_elapsed_seconds`` / ``_seconds_after`` 等，为了 DST fold 正确性）。
北辰运行在 Asia/Shanghai —— 固定 UTC+8、无 DST —— 所以新旧表达式**必须**等价。
本文件把这条等价性也钉死，升级不是靠「中国没有 DST」口头推断过关。
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from cron import jobs


# S5 的真实形状：2026-09-24 暂停，约 13 天后恢复。
_PAUSED_LAG = timedelta(days=13)


def _store(tmp_path, monkeypatch, *, config: str = ""):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    if config:
        (tmp_path / "config.yaml").write_text(config, encoding="utf-8")
    return jobs.use_cron_store(tmp_path / "cron")


def _row(job_id: str) -> dict:
    return next(row for row in jobs.load_jobs() if row["id"] == job_id)


def _set_next_run(job_id: str, value: str) -> None:
    stored = jobs.load_jobs()
    next(row for row in stored if row["id"] == job_id)["next_run_at"] = value
    jobs.save_jobs(stored)


def _legal_past_slot(job: dict, *, days_ago: int = 13) -> str:
    """A ``days_ago``-days-earlier **legal** occurrence of a daily cron job.

    Must be on the expression's lattice: ``_reanchor_stale_cron`` treats an
    off-lattice ``next_run_at`` as a direct jobs.json edit and re-anchors it
    WITHOUT firing. The 56 paused S5 jobs carry real lattice points, and the
    execution package writes ``compute_next_run()`` output, which is also one —
    so the realistic fixture is a lattice point, not an arbitrary timestamp.
    """
    natural = jobs._parse_aware(jobs.compute_next_run(job["schedule"]))
    return (natural - timedelta(days=days_ago)).isoformat()


# ---------------------------------------------------------------- resume 语义

@pytest.mark.parametrize("schedule", ["0 8 * * *", "every 1h"])
def test_resume_keeps_the_elapsed_occurrence_verbatim(tmp_path, monkeypatch, schedule):
    """cron 与 interval 两种 kind 的过期 slot 都原样留下，不重算。

    这是 S5 的核心依赖：如果 resume 把 next_run_at 重锚到未来，56 个 job 的
    「这一期该不该补」就被 resume 静默裁决了，catch_up 策略根本不会被征询。
    """
    with _store(tmp_path, monkeypatch):
        job = jobs.create_job(prompt="s5", schedule=schedule, model="fixture", deliver="local")
        elapsed = (jobs._hermes_now() - _PAUSED_LAG).isoformat()
        jobs.pause_job(job["id"], reason="travel-mode 2026-09-24")
        _set_next_run(job["id"], elapsed)

        resumed = jobs.resume_job(job["id"])

        assert resumed["next_run_at"] == elapsed, "过期 slot 必须逐字符保留"
        assert resumed["enabled"] is True
        assert resumed["state"] == "scheduled"
        assert resumed["paused_at"] is None
        assert resumed["paused_reason"] is None
        assert _row(job["id"])["next_run_at"] == elapsed, "落盘的也必须是过期 slot"


def test_resume_recomputes_only_a_future_occurrence(tmp_path, monkeypatch):
    """未来时刻才重算 —— 过期与未来两条分支的边界不能反。"""
    with _store(tmp_path, monkeypatch):
        job = jobs.create_job(prompt="future", schedule="0 8 * * *", model="fixture", deliver="local")
        future = (jobs._hermes_now() + timedelta(days=3)).isoformat()
        jobs.pause_job(job["id"])
        _set_next_run(job["id"], future)

        resumed = jobs.resume_job(job["id"])

        assert resumed["next_run_at"] != future
        assert jobs._parse_aware(resumed["next_run_at"]) > jobs._hermes_now()


# ------------------------------------------------------- catch-up 策略与 due 扫描

def test_resumed_overdue_job_catches_up_exactly_once_by_default(tmp_path, monkeypatch):
    """默认（无 cron.catch_up_missed 配置）：补跑一次并把 next_run_at 前推。

    「前推」是防雪崩的关键：两周 ≈ 13 个日更 slot。这里同时钉死一条更强的性质 ——
    即使 dispatcher 一直不 claim（本测试就是这种情形，``_restore_unclaimed_slot``
    会把未被 claim 的 slot 放回来，#107485），反复扫描始终只围绕**同一个** slot，
    绝不沿着 13 天逐 slot 回放。
    """
    with _store(tmp_path, monkeypatch):
        job = jobs.create_job(prompt="s5-daily", schedule="0 8 * * *", model="fixture", deliver="local")
        jobs.pause_job(job["id"], reason="travel-mode 2026-09-24")
        elapsed = _legal_past_slot(job)
        _set_next_run(job["id"], elapsed)
        jobs.resume_job(job["id"])

        assert [row["id"] for row in jobs.get_due_jobs()] == [job["id"]], "补跑这一次"
        pushed = _row(job["id"])["next_run_at"]
        assert jobs._parse_aware(pushed) > jobs._hermes_now(), "同一轮必须把 next_run_at 推到未来"

        # 再扫 4 轮：始终是同一个 slot，next_run_at 不再往前走第二步。
        slots = set()
        for _ in range(4):
            jobs.get_due_jobs()
            row = _row(job["id"])
            assert row["next_run_at"] == pushed, "不得逐 slot 前推（那就是雪崩）"
            slots.add((row.get("pending_slot") or {}).get("scheduled_at"))
        assert len(slots) == 1, f"反复扫描只应围绕同一个 slot，实际 {slots}"
        assert slots == {elapsed}, "放回来的必须正是那一期，不是别的 slot"


def test_catch_up_missed_false_skips_the_elapsed_occurrence(tmp_path, monkeypatch):
    """显式关掉 catch_up_missed：不补跑，但仍然前推（不留在过去空转）。"""
    with _store(tmp_path, monkeypatch, config="cron:\n  catch_up_missed: false\n"):
        job = jobs.create_job(prompt="s5-skip", schedule="0 8 * * *", model="fixture", deliver="local")
        jobs.pause_job(job["id"])
        _set_next_run(job["id"], _legal_past_slot(job))
        jobs.resume_job(job["id"])

        assert jobs.get_due_jobs() == []
        assert jobs._parse_aware(_row(job["id"])["next_run_at"]) > jobs._hermes_now()


def test_a_next_run_off_the_cron_lattice_is_reanchored_without_firing(tmp_path, monkeypatch):
    """S5 必须写 compute_next_run() 的输出，不能手搓时间戳。

    off-lattice 的 next_run_at 被 ``_reanchor_stale_cron`` 当成直接改 jobs.json，
    静默前推且**不触发**这一期 —— 对 56 个 job 的恢复来说那就是静默丢一期。
    这条是执行包「只写 compute_next_run 输出」这个纪律的机器化依据。
    """
    with _store(tmp_path, monkeypatch):
        job = jobs.create_job(prompt="off-lattice", schedule="0 8 * * *", model="fixture", deliver="local")
        jobs.pause_job(job["id"])
        legal = jobs._parse_aware(_legal_past_slot(job))
        _set_next_run(job["id"], (legal + timedelta(minutes=37)).isoformat())
        jobs.resume_job(job["id"])

        assert jobs.get_due_jobs() == [], "off-lattice 不触发"
        assert jobs._parse_aware(_row(job["id"])["next_run_at"]) > jobs._hermes_now()


def test_catch_up_missed_defaults_to_true(tmp_path, monkeypatch):
    """默认值本身也钉死：S5 执行包是按 True 推演的。"""
    with _store(tmp_path, monkeypatch):
        assert jobs._cron_config_number("catch_up_missed", True, lambda v: v is not False) is True


def test_disabled_job_with_a_due_next_run_is_not_due(tmp_path, monkeypatch):
    """A1 两步法的安全前提：先改 next_run_at、此刻仍 disabled，不会被 due 扫描捞走。

    用 interval kind，因为 interval 不受 cron lattice 守卫影响，能把「两步法本身」
    和「写的值合不合法」这两件事分开验。
    """
    with _store(tmp_path, monkeypatch):
        job = jobs.create_job(prompt="two-step", schedule="every 1h", model="fixture", deliver="local")
        jobs.pause_job(job["id"])
        _set_next_run(job["id"], (jobs._hermes_now() - timedelta(minutes=5)).isoformat())

        assert jobs.get_due_jobs() == [], "enabled=False 期间改 next_run_at 必须是安全的"

        jobs.resume_job(job["id"])
        assert [row["id"] for row in jobs.get_due_jobs()] == [job["id"]]


# ---------------------------------------------------------------- grace 窗口

@pytest.mark.parametrize(
    "schedule, expected",
    [
        ("every 1m", 120),      # 30s → clamp 到下界
        ("every 10m", 300),     # 周期一半
        ("every 1h", 1800),
        ("0 8 * * *", 7200),    # 日更：43200s 的一半 → clamp 到上界
        ("0 8 * * 1", 7200),    # 周更同样顶到上界
    ],
)
def test_grace_is_half_the_period_clamped_to_120s_7200s(tmp_path, monkeypatch, schedule, expected):
    with _store(tmp_path, monkeypatch):
        job = jobs.create_job(prompt="grace", schedule=schedule, model="fixture", deliver="local")
        assert jobs._compute_grace_seconds(_row(job["id"])["schedule"]) == expected
        assert jobs._MIN_GRACE_SECONDS == 120
        assert jobs._MAX_GRACE_SECONDS == 7200


def test_s5_lag_is_far_beyond_any_grace_window(tmp_path, monkeypatch):
    """13 天的迟到必须落在 catch_up 档，而不是 on_time / late 档。"""
    with _store(tmp_path, monkeypatch):
        lateness = _PAUSED_LAG.total_seconds()
        assert jobs._classify_dispatch_lateness(lateness, 7200) == "catch_up"
        assert jobs._classify_dispatch_lateness(60, 7200) == "on_time"
        assert jobs._classify_dispatch_lateness(1800, 7200) == "late"


# ------------------------------- v2026.9.24 instant helper 与旧 wall-clock 等价性

_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _shanghai_instants():
    """跨越 S5 关心的区间（含北半球 DST 切换日），全部用 Asia/Shanghai 表达。"""
    anchors = [
        datetime(2026, 9, 24, 8, 0, tzinfo=_SHANGHAI),   # 暂停日
        datetime(2026, 10, 7, 16, 0, tzinfo=_SHANGHAI),  # 恢复日
        datetime(2026, 10, 8, 8, 0, tzinfo=_SHANGHAI),   # 首个交易日
        datetime(2026, 11, 1, 9, 30, tzinfo=_SHANGHAI),  # 美国 DST fall-back 当天
        datetime(2026, 3, 8, 9, 30, tzinfo=_SHANGHAI),   # 美国 DST spring-forward 当天
    ]
    pairs = []
    for a in anchors:
        for delta in (timedelta(0), timedelta(seconds=1), timedelta(hours=1),
                      timedelta(days=1), _PAUSED_LAG):
            pairs.append((a, a - delta))
            pairs.append((a, a + delta))
    return pairs


def test_instant_helpers_equal_wall_clock_arithmetic_in_asia_shanghai():
    """固定 UTC+8 无 DST，新 helper 与旧表达式必须逐例相等。

    这条等价性是「升级不改变 S5 行为」的直接依据：升级把 resume/due/catch-up
    路径上的 ``<=`` ``>`` ``-`` ``+timedelta`` 全换成了 helper。
    """
    for later, earlier in _shanghai_instants():
        assert jobs._instant_at_or_before(earlier, later) == (earlier <= later)
        assert jobs._instant_after(earlier, later) == (earlier > later)
        assert jobs._instant_before(earlier, later) == (earlier < later)
        assert jobs._elapsed_seconds(later, earlier) == pytest.approx(
            (later - earlier).total_seconds())
    for base, _ in _shanghai_instants():
        for seconds in (0, 120, 7200, int(_PAUSED_LAG.total_seconds())):
            assert jobs._seconds_after(base, seconds) == base + timedelta(seconds=seconds)


def test_instant_helpers_normalise_mixed_zones():
    """helper 的契约是「绝对时刻」，所以跨时区表达同一瞬间必须判等。"""
    shanghai = datetime(2026, 10, 8, 8, 0, tzinfo=_SHANGHAI)
    same_utc = shanghai.astimezone(timezone.utc)
    assert jobs._instant_at_or_before(shanghai, same_utc)
    assert jobs._instant_at_or_before(same_utc, shanghai)
    assert not jobs._instant_before(shanghai, same_utc)
    assert jobs._elapsed_seconds(same_utc, shanghai) == 0.0
