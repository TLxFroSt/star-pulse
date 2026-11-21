"""Tests du client HTTP : retries, backoff et requêtes conditionnelles."""

import httpx
import pytest

from star_pulse.common.http import RetryPolicy, build_client, get_with_retry
from star_pulse.config import Settings

URL = "https://example.test/feed"


def _client(handler: httpx.MockTransport) -> httpx.Client:
    return build_client(Settings(), transport=handler)


def test_retries_transient_status_then_succeeds() -> None:
    statuses = iter([503, 502, 200])
    sleeps: list[float] = []
    transport = httpx.MockTransport(lambda _: httpx.Response(next(statuses)))

    with _client(transport) as client:
        response = get_with_retry(client, URL, sleep=sleeps.append)

    assert response.status_code == 200
    assert sleeps == [2.0, 4.0]


def test_retries_network_errors() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("boom", request=request)
        return httpx.Response(200)

    with _client(httpx.MockTransport(handler)) as client:
        assert get_with_retry(client, URL, sleep=lambda _: None).status_code == 200
    assert calls == 2


def test_gives_up_after_max_attempts() -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(503))
    policy = RetryPolicy(max_attempts=3)

    with _client(transport) as client, pytest.raises(httpx.HTTPStatusError):
        get_with_retry(client, URL, policy=policy, sleep=lambda _: None)


def test_client_error_is_not_retried() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404)

    with _client(httpx.MockTransport(handler)) as client, pytest.raises(httpx.HTTPStatusError):
        get_with_retry(client, URL, sleep=lambda _: None)
    assert calls == 1


def test_not_modified_is_returned_and_user_agent_is_sent() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(request.headers)
        return httpx.Response(304)

    with _client(httpx.MockTransport(handler)) as client:
        response = get_with_retry(client, URL, headers={"If-None-Match": '"abc"'})

    assert response.status_code == 304
    assert seen["if-none-match"] == '"abc"'
    assert seen["user-agent"].startswith("star-pulse/")


def test_backoff_is_capped() -> None:
    policy = RetryPolicy(base_delay_s=10, max_delay_s=25)
    assert [policy.delay(n) for n in (1, 2, 3)] == [10.0, 20.0, 25.0]
