"""
Test suite for the Alerting API Service.

- Validation unit tests: call _validate_reading directly, no DB/network needed.
- Ingestion endpoint tests: partial-accept shape, empty batch, case sensitivity.
- Aggregation tests: seed via /metrics/ingest, assert DB-computed agg values.
- Query endpoint tests: raw list shape, empty-result agg (no 500).
- Alert rule endpoint tests: create, validate, list.
- Rule evaluator state-machine tests: timer start, sustained violation, no
  duplicate, resolution, full-duration reset, global (host_id=None) rule.
- Async decoupling test: patch evaluate_rules to sleep 2s, assert ingest
  response stays under 1000ms — the shadow-dispatch proof.
"""

import asyncio
import time
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest
from httpx import AsyncClient, ASGITransport

from main import app, _validate_reading


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _past(minutes: int = 5) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


def _future(hours: int = 1) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def _valid_raw(**overrides) -> dict:
    base = {
        "host_id": "web-01",
        "metric_name": "cpu",
        "value": 50.0,
        "timestamp": _past(),
    }
    return {**base, **overrides}


def _uid() -> str:
    """Unique host_id so agg tests don't bleed into each other."""
    return f"t-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Validation unit tests (no DB)
# ---------------------------------------------------------------------------

def test_valid_reading_passes():
    reading, err = _validate_reading(_valid_raw())
    assert reading is not None and err is None


def test_rejects_empty_host_id():
    _, err = _validate_reading(_valid_raw(host_id=""))
    assert err and "host_id" in err


def test_rejects_whitespace_host_id():
    _, err = _validate_reading(_valid_raw(host_id="   "))
    assert err and "host_id" in err


def test_rejects_non_string_host_id():
    _, err = _validate_reading(_valid_raw(host_id=42))
    assert err and "host_id" in err


def test_rejects_invalid_metric_name():
    _, err = _validate_reading(_valid_raw(metric_name="network"))
    assert err and "metric_name" in err


def test_rejects_boolean_value():
    # bool is a subclass of int in Python — must be explicitly rejected
    _, err = _validate_reading(_valid_raw(value=True))
    assert err and "value" in err


def test_rejects_string_value():
    _, err = _validate_reading(_valid_raw(value="high"))
    assert err and "value" in err


def test_rejects_none_value():
    _, err = _validate_reading(_valid_raw(value=None))
    assert err is not None


def test_rejects_non_string_timestamp():
    _, err = _validate_reading(_valid_raw(timestamp=1234567890))
    assert err and "timestamp" in err


def test_rejects_malformed_timestamp():
    _, err = _validate_reading(_valid_raw(timestamp="not-a-date"))
    assert err and "timestamp" in err


def test_rejects_future_timestamp_beyond_5min():
    _, err = _validate_reading(_valid_raw(timestamp=_future()))
    assert err and "future" in err


def test_accepts_timestamp_just_within_5min_window():
    # 4 minutes in the future — still valid
    ts = (datetime.now(timezone.utc) + timedelta(minutes=4)).isoformat()
    reading, err = _validate_reading(_valid_raw(timestamp=ts))
    assert reading is not None and err is None


def test_rejects_non_dict_entry():
    _, err = _validate_reading("not a dict")
    assert err is not None


def test_strips_host_id_whitespace():
    reading, err = _validate_reading(_valid_raw(host_id="  web-01  "))
    assert err is None and reading.host_id == "web-01"


# ---------------------------------------------------------------------------
# Aggregation integration tests (real DB, unique host per test)
# ---------------------------------------------------------------------------

async def _seed_and_query(host_id: str, metric: str, values: list[float], agg: str) -> float:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        now = datetime.now(timezone.utc)
        readings = [
            {
                "host_id": host_id,
                "metric_name": metric,
                "value": v,
                "timestamp": (now - timedelta(minutes=i + 1)).isoformat(),
            }
            for i, v in enumerate(values)
        ]
        r = await client.post("/metrics/ingest", json={"readings": readings})
        assert r.json()["accepted"] == len(values)

        r = await client.get(
            "/metrics/query",
            params={"host_id": host_id, "metric": metric, "window": "1h", "agg": agg},
        )
        assert r.status_code == 200
        return r.json()["value"]


async def test_agg_avg():
    # (10 + 20 + 30) / 3 = 20.0
    result = await _seed_and_query(_uid(), "cpu", [10.0, 20.0, 30.0], "avg")
    assert result == pytest.approx(20.0)


async def test_agg_min():
    result = await _seed_and_query(_uid(), "memory", [40.0, 10.0, 70.0], "min")
    assert result == pytest.approx(10.0)


async def test_agg_max():
    result = await _seed_and_query(_uid(), "disk", [40.0, 10.0, 70.0], "max")
    assert result == pytest.approx(70.0)


async def test_agg_window_excludes_old_readings():
    """Readings older than the window must not affect the aggregate."""
    host_id = _uid()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        now = datetime.now(timezone.utc)
        readings = [
            # Within 30m window — only these should count
            {"host_id": host_id, "metric_name": "cpu", "value": 50.0,
             "timestamp": (now - timedelta(minutes=10)).isoformat()},
            {"host_id": host_id, "metric_name": "cpu", "value": 60.0,
             "timestamp": (now - timedelta(minutes=20)).isoformat()},
            # Outside 30m window — must be excluded
            {"host_id": host_id, "metric_name": "cpu", "value": 999.0,
             "timestamp": (now - timedelta(minutes=60)).isoformat()},
        ]
        await client.post("/metrics/ingest", json={"readings": readings})

        r = await client.get(
            "/metrics/query",
            params={"host_id": host_id, "metric": "cpu", "window": "30m", "agg": "avg"},
        )
        assert r.json()["value"] == pytest.approx(55.0)  # (50+60)/2, not (50+60+999)/3


# ---------------------------------------------------------------------------
# Async decoupling integration test — the shadow-dispatch proof
# ---------------------------------------------------------------------------

async def test_ingest_latency_unaffected_by_slow_evaluator():
    """
    evaluate_rules is patched to sleep 2 seconds.
    The ingest response must still return in < 1000ms.

    If asyncio.create_task() were replaced with await, this test would take
    2+ seconds and fail. The sub-1000ms assertion IS the decoupling proof
    (threshold accounts for DO Postgres round-trip latency).
    """

    async def slow_evaluate(host_id: str, metric_name: str) -> None:
        await asyncio.sleep(2.0)

    with patch("main.evaluate_rules", slow_evaluate):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            now = datetime.now(timezone.utc).isoformat()
            start = time.monotonic()
            resp = await client.post(
                "/metrics/ingest",
                json={"readings": [
                    {"host_id": "perf-host", "metric_name": "cpu", "value": 50.0, "timestamp": now}
                ]},
            )
            elapsed_ms = (time.monotonic() - start) * 1000

    assert resp.status_code == 200
    assert resp.json()["accepted"] == 1
    # Threshold is 1000ms — well above the ~250ms DO Postgres round-trip but
    # well below the 2000ms evaluator sleep. If create_task() were replaced with
    # await, elapsed would be 2000ms+ and this assertion would fail.
    assert elapsed_ms < 1000, (
        f"Ingest took {elapsed_ms:.1f}ms with a 2s evaluator — "
        "evaluate_rules is blocking the response path (should use create_task)"
    )


# ---------------------------------------------------------------------------
# Ingestion endpoint integration tests
# ---------------------------------------------------------------------------

async def test_ingest_partial_accept_via_endpoint():
    """Endpoint wires validation correctly: right accepted/rejected counts and indices."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/metrics/ingest", json={"readings": [
            {"host_id": "h1", "metric_name": "cpu",    "value": 50.0, "timestamp": _past()},
            {"host_id": "",   "metric_name": "cpu",    "value": 50.0, "timestamp": _past()},
            {"host_id": "h2", "metric_name": "memory", "value": 60.0, "timestamp": _past()},
        ]})
        assert r.status_code == 200
        body = r.json()
        assert body["accepted"] == 2
        assert len(body["rejected"]) == 1
        assert body["rejected"][0]["index"] == 1


async def test_ingest_empty_batch():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/metrics/ingest", json={"readings": []})
        assert r.status_code == 200
        assert r.json() == {"accepted": 0, "rejected": []}


def test_ingest_rejects_uppercase_metric_name():
    # metric_name is case-sensitive; "CPU" is not a valid metric
    _, err = _validate_reading(_valid_raw(metric_name="CPU"))
    assert err and "metric_name" in err


# ---------------------------------------------------------------------------
# Query endpoint integration tests
# ---------------------------------------------------------------------------

async def test_query_without_agg_returns_raw_list():
    host_id = _uid()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await client.post("/metrics/ingest", json={"readings": [
            {"host_id": host_id, "metric_name": "cpu", "value": 42.0, "timestamp": _past()}
        ]})
        r = await client.get("/metrics/query", params={"host_id": host_id, "metric": "cpu", "window": "1h"})
        assert r.status_code == 200
        body = r.json()
        assert "readings" in body
        assert isinstance(body["readings"], list)
        assert body["readings"][0]["value"] == 42.0


async def test_agg_avg_on_empty_result_returns_none_not_500():
    """No rows in window → avg returns null, not a divide-by-zero 500."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/metrics/query", params={
            "host_id": f"ghost-{uuid.uuid4().hex}",
            "metric": "cpu",
            "window": "1h",
            "agg": "avg",
        })
        assert r.status_code == 200
        assert r.json()["value"] is None


# ---------------------------------------------------------------------------
# Alert rule endpoint tests
# ---------------------------------------------------------------------------

async def test_create_rule_returns_id_and_fields():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/alerts/rules", json={
            "host_id": _uid(), "metric_name": "cpu",
            "operator": ">", "threshold": 90.0, "duration_seconds": 300,
        })
        assert r.status_code == 201
        body = r.json()
        assert isinstance(body["id"], int)
        assert body["metric_name"] == "cpu"
        assert body["operator"] == ">"
        assert body["threshold"] == 90.0
        assert body["active"] is True


async def test_create_rule_rejects_invalid_metric():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/alerts/rules", json={
            "metric_name": "network", "operator": ">", "threshold": 90.0, "duration_seconds": 60
        })
        assert r.status_code == 400


async def test_create_rule_rejects_invalid_operator():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/alerts/rules", json={
            "metric_name": "cpu", "operator": "!=", "threshold": 90.0, "duration_seconds": 60
        })
        assert r.status_code == 400


async def test_create_rule_rejects_non_positive_duration():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/alerts/rules", json={
            "metric_name": "cpu", "operator": ">", "threshold": 90.0, "duration_seconds": 0
        })
        assert r.status_code == 400


async def test_list_rules_includes_created_rule():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/alerts/rules", json={
            "host_id": _uid(), "metric_name": "disk",
            "operator": ">=", "threshold": 95.0, "duration_seconds": 120,
        })
        rule_id = r.json()["id"]
        r = await client.get("/alerts/rules")
        assert r.status_code == 200
        ids = [rule["id"] for rule in r.json()]
        assert rule_id in ids


# ---------------------------------------------------------------------------
# Rule evaluator state-machine tests
#
# Pattern: create a rule with duration_seconds=1, ingest readings, await short
# sleeps to let background tasks (create_task + run_in_executor) complete, then
# assert on /alerts/violations filtered by host_id + rule_id.
# ---------------------------------------------------------------------------

def _ingest_reading(host_id: str, metric: str, value: float) -> dict:
    return {"host_id": host_id, "metric_name": metric, "value": value,
            "timestamp": datetime.now(timezone.utc).isoformat()}


async def _violations_for(client, host_id: str, rule_id: int) -> list:
    r = await client.get("/alerts/violations")
    return [v for v in r.json() if v["host_id"] == host_id and v["rule_id"] == rule_id]


async def test_single_breach_does_not_immediately_violate():
    """Timer starts on first breach but no violation until duration_seconds elapses."""
    host_id = _uid()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/alerts/rules", json={
            "host_id": host_id, "metric_name": "cpu",
            "operator": ">", "threshold": 80.0, "duration_seconds": 60,
        })
        rule_id = r.json()["id"]

        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 95.0)]})
        await asyncio.sleep(2.0)  # Let evaluator run — timer starts, but 60s hasn't elapsed

        assert await _violations_for(client, host_id, rule_id) == []


async def test_sustained_breach_creates_violation():
    """Two breach readings separated by > duration_seconds → violation logged."""
    host_id = _uid()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/alerts/rules", json={
            "host_id": host_id, "metric_name": "cpu",
            "operator": ">", "threshold": 80.0, "duration_seconds": 1,
        })
        rule_id = r.json()["id"]

        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 95.0)]})
        await asyncio.sleep(2.0)  # Evaluator runs (starts timer); duration elapses

        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 97.0)]})
        await asyncio.sleep(2.0)  # Evaluator runs → logs violation

        violations = await _violations_for(client, host_id, rule_id)
        assert len(violations) == 1
        assert violations[0]["value"] == 97.0
        assert violations[0]["resolved_at"] is None


async def test_no_duplicate_violation_while_still_breaching():
    """A third above-threshold ingest while already violating must not create a second row."""
    host_id = _uid()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/alerts/rules", json={
            "host_id": host_id, "metric_name": "cpu",
            "operator": ">", "threshold": 80.0, "duration_seconds": 1,
        })
        rule_id = r.json()["id"]

        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 95.0)]})
        await asyncio.sleep(2.0)
        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 97.0)]})
        await asyncio.sleep(2.0)
        assert len(await _violations_for(client, host_id, rule_id)) == 1

        # Third ingest — still over threshold, violation already open
        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 98.0)]})
        await asyncio.sleep(2.0)

        assert len(await _violations_for(client, host_id, rule_id)) == 1  # Still one


async def test_violation_resolves_when_below_threshold():
    """Reading that drops below threshold sets resolved_at on the open violation."""
    host_id = _uid()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/alerts/rules", json={
            "host_id": host_id, "metric_name": "cpu",
            "operator": ">", "threshold": 80.0, "duration_seconds": 1,
        })
        rule_id = r.json()["id"]

        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 95.0)]})
        await asyncio.sleep(2.0)
        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 97.0)]})
        await asyncio.sleep(2.0)
        assert len(await _violations_for(client, host_id, rule_id)) == 1

        # Drop below threshold
        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 50.0)]})
        await asyncio.sleep(2.0)

        violations = await _violations_for(client, host_id, rule_id)
        assert violations[0]["resolved_at"] is not None


async def test_crossing_again_after_reset_requires_full_duration():
    """After resolution, re-crossing threshold does not fire instantly — timer restarts."""
    host_id = _uid()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/alerts/rules", json={
            "host_id": host_id, "metric_name": "cpu",
            "operator": ">", "threshold": 80.0, "duration_seconds": 1,
        })
        rule_id = r.json()["id"]

        # Create and resolve a violation
        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 95.0)]})
        await asyncio.sleep(2.0)
        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 97.0)]})
        await asyncio.sleep(2.0)
        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 50.0)]})
        await asyncio.sleep(2.0)
        assert (await _violations_for(client, host_id, rule_id))[0]["resolved_at"] is not None

        # Immediately cross threshold again — duration_seconds=1 has NOT elapsed yet
        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "cpu", 95.0)]})
        await asyncio.sleep(0.5)  # Short sleep — timer just restarted, violation must not fire

        # Must still be only one violation (the original resolved one); no new one yet
        assert len(await _violations_for(client, host_id, rule_id)) == 1


async def test_global_rule_applies_to_arbitrary_host():
    """A rule with host_id=None fires for any host, not just a named one."""
    host_id = _uid()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/alerts/rules", json={
            "host_id": None, "metric_name": "disk",
            "operator": ">", "threshold": 70.0, "duration_seconds": 1,
        })
        rule_id = r.json()["id"]

        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "disk", 85.0)]})
        await asyncio.sleep(2.0)
        await client.post("/metrics/ingest", json={"readings": [_ingest_reading(host_id, "disk", 90.0)]})
        await asyncio.sleep(2.0)

        violations = await _violations_for(client, host_id, rule_id)
        assert len(violations) == 1
        assert violations[0]["host_id"] == host_id
