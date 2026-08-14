"""Strict BLOCKED receipts may satisfy the artifact contract without prose."""

import pytest

import cron.scheduler as scheduler


def _artifact_job(tmp_path, *, minimum_chars=1000):
    return {
        "id": "blocked-receipt",
        "workdir": str(tmp_path),
        "artifact_path": "receipt.json",
        "artifact_min_chars": minimum_chars,
    }


def test_strict_blocked_receipt_bypasses_minimum_length_byte_for_byte(tmp_path):
    response = (
        '{\n  "status": "BLOCKED",\n'
        '  "reason_code": "UPSTREAM_DATA_UNAVAILABLE",\n'
        '  "decision_eligible": false\n}\n'
    )

    target = scheduler._write_job_artifact(_artifact_job(tmp_path), response)

    assert target.read_bytes() == response.encode("utf-8")


@pytest.mark.parametrize(
    "response",
    [
        "BLOCKED",
        '{"status":"BLOCKED","decision_eligible":false}',
        '{"status":"BLOCKED","reason_code":"DATA_UNAVAILABLE"}',
        '{"status":"BLOCKED","reason_code":"DATA_UNAVAILABLE",'
        '"decision_eligible":true}',
        '{"status":"BLOCKED","reason_code":"data_unavailable",'
        '"decision_eligible":false}',
        '{"status":"BLOCKED","reason_code":"DATA_UNAVAILABLE",'
        '"decision_eligible":false,"detail":"extra"}',
        '{"status":"BLOCKED","status":"BLOCKED",'
        '"reason_code":"DATA_UNAVAILABLE","decision_eligible":false}',
        '[{"status":"BLOCKED","reason_code":"DATA_UNAVAILABLE",'
        '"decision_eligible":false}]',
        'prefix {"status":"BLOCKED","reason_code":"DATA_UNAVAILABLE",'
        '"decision_eligible":false}',
        '{"status":"BLOCKED","reason_code":"DATA_UNAVAILABLE",'
        '"decision_eligible":false} suffix',
        '{"status":"BLOCKED","reason_code":"DATA_UNAVAILABLE",',
        "   ",
        "ordinary short response",
    ],
)
def test_non_receipt_short_responses_remain_rejected(tmp_path, response):
    with pytest.raises(ValueError, match="artifact_min_chars"):
        scheduler._write_job_artifact(_artifact_job(tmp_path), response)

    assert not (tmp_path / "receipt.json").exists()


def test_ordinary_long_response_keeps_existing_behavior(tmp_path):
    response = "ordinary response"

    target = scheduler._write_job_artifact(
        _artifact_job(tmp_path, minimum_chars=len(response)),
        response,
    )

    assert target.read_text(encoding="utf-8") == response
