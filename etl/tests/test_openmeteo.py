import json
from datetime import date
from pathlib import Path

import httpx
import pytest
import respx
from weather_etl.openmeteo import (
    HOURLY_VARS,
    ArchiveClient,
    OpenMeteoError,
    QuotaExceeded,
    call_weight,
)

FIXTURE = Path(__file__).parent / "fixtures" / "archive_vienna_2023-01-01_02.json"
D1, D2 = date(2023, 1, 1), date(2023, 1, 2)


def route():
    return respx.get(host="archive-api.open-meteo.com", path="/v1/archive")


def limit(reason: str) -> httpx.Response:
    return httpx.Response(429, json={"error": True, "reason": reason})


class RecordingBudget:
    def __init__(self):
        self.weights = []

    def acquire(self, weight):
        self.weights.append(weight)


@pytest.fixture
def sleeps():
    return []


@pytest.fixture
def client(sleeps):
    return ArchiveClient(sleep=sleeps.append)


def test_call_weight():
    assert call_weight(12, D1, D2) == pytest.approx(1.2)
    assert call_weight(12, date(2023, 1, 1), date(2023, 12, 31)) == pytest.approx(1.2 * 365 / 14)
    assert call_weight(5, D1, D1) == 1.0
    with pytest.raises(ValueError):
        call_weight(12, D2, D1)


@respx.mock
def test_fetch_returns_payload_and_sends_expected_params(client):
    r = route().mock(return_value=httpx.Response(200, text=FIXTURE.read_text()))
    res = client.fetch_hourly(48.21, 16.37, D1, D2)
    assert json.loads(res.payload)["hourly"]["time"][0] == "2023-01-01T00:00"
    params = r.calls.last.request.url.params
    assert params["models"] == "era5"
    assert params["timezone"] == "UTC"
    assert params["start_date"] == "2023-01-01" and params["end_date"] == "2023-01-02"
    assert params["hourly"].split(",") == list(HOURLY_VARS)
    assert res.weight == pytest.approx(1.2)
    assert res.fetched_at.tzinfo is not None


@respx.mock
def test_minutely_limit_backs_off_then_succeeds(client, sleeps):
    r = route()
    r.side_effect = [
        limit("Minutely API request limit exceeded. Please try again in one minute."),
        httpx.Response(200, text=FIXTURE.read_text()),
    ]
    client.fetch_hourly(48.21, 16.37, D1, D2)
    assert r.call_count == 2
    assert sleeps == [20.0]


@respx.mock
def test_daily_limit_raises_quota_without_retry(client):
    r = route().mock(
        return_value=limit("Daily API request limit exceeded. Please try again tomorrow.")
    )
    with pytest.raises(QuotaExceeded) as exc:
        client.fetch_hourly(48.21, 16.37, D1, D2)
    assert exc.value.scope == "day"
    assert r.call_count == 1


@respx.mock
def test_hourly_limit_raises_quota(client):
    route().mock(
        return_value=limit("Hourly API request limit exceeded. Please try again in the next hour.")
    )
    with pytest.raises(QuotaExceeded) as exc:
        client.fetch_hourly(48.21, 16.37, D1, D2)
    assert exc.value.scope == "hour"


@respx.mock
def test_server_errors_retry_then_give_up(client, sleeps):
    r = route().mock(return_value=httpx.Response(503, text="unavailable"))
    with pytest.raises(OpenMeteoError, match="gave up after 3 attempts"):
        client.fetch_hourly(48.21, 16.37, D1, D2)
    assert r.call_count == 3
    assert sleeps == [20.0, 40.0]


@respx.mock
def test_bad_request_fails_fast_with_reason(client):
    route().mock(return_value=httpx.Response(400, json={"error": True, "reason": "Invalid value"}))
    with pytest.raises(OpenMeteoError, match="HTTP 400: Invalid value"):
        client.fetch_hourly(48.21, 16.37, D1, D2)


@respx.mock
def test_transport_error_is_retried(client):
    r = route()
    r.side_effect = [httpx.ConnectError("boom"), httpx.Response(200, text=FIXTURE.read_text())]
    client.fetch_hourly(48.21, 16.37, D1, D2)
    assert r.call_count == 2


@respx.mock
def test_budget_charged_per_attempt(sleeps):
    budget = RecordingBudget()
    r = route()
    r.side_effect = [
        limit("Minutely API request limit exceeded."),
        httpx.Response(200, text=FIXTURE.read_text()),
    ]
    ArchiveClient(budget=budget, sleep=sleeps.append).fetch_hourly(48.21, 16.37, D1, D2)
    assert budget.weights == [pytest.approx(1.2), pytest.approx(1.2)]
