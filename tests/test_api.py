from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from conduit.db import APIKey
from conduit.providers.mock import mock_completion
from conduit.schemas import ChatCompletionRequest
from tests.conftest import ADMIN, AUTH, chat_body, open_gateway


def _events(body: str) -> list[object]:
    events: list[object] = []
    for block in body.split("\n\n"):
        for line in block.splitlines():
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            events.append("[DONE]" if data == "[DONE]" else json.loads(data))
    return events


def _stream_text(body: str) -> str:
    parts: list[str] = []
    for event in _events(body):
        if not isinstance(event, dict):
            continue
        for choice in event.get("choices") or []:
            content = (choice.get("delta") or {}).get("content")
            if isinstance(content, str):
                parts.append(content)
    return "".join(parts)


async def test_health_models_and_non_streaming_chat(gateway) -> None:
    client, _app = gateway
    health = await client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    missing = await client.get("/v1/models")
    assert missing.status_code == 401
    models = await client.get("/v1/models", headers=AUTH)
    assert "mock" in {item["id"] for item in models.json()["data"]}
    response = await client.post("/v1/chat/completions", headers=AUTH, json=chat_body())
    assert response.status_code == 200
    body = response.json()
    assert body["object"] == "chat.completion"
    assert body["model"] == "mock"
    assert body["choices"][0]["message"]["content"].startswith("conduit-mock:")
    assert body["usage"]["total_tokens"] > 0
    assert response.headers["x-conduit-provider"] == "primary"
    assert response.headers["x-conduit-cache"] == "miss"
    home = await client.get("/")
    assert home.status_code == 200
    assert "Conduit" in home.text
    admin = await client.get("/admin")
    assert admin.status_code == 200
    assert "Keys and usage" in admin.text


async def test_streaming_replays_a_deterministic_completion(gateway) -> None:
    client, _app = gateway
    response = await client.post("/v1/chat/completions", headers=AUTH, json=chat_body(stream=True))
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    events = _events(response.text)
    assert events[-1] == "[DONE]"
    assert any(isinstance(event, dict) and event.get("usage") for event in events)
    expected, _prompt, _completion = mock_completion(
        ChatCompletionRequest(model="mock", messages=[{"role": "user", "content": "hello"}]),
        "echo",
    )
    assert _stream_text(response.text) == expected


async def test_exact_cache_and_bypass(gateway) -> None:
    client, app = gateway
    first = await client.post("/v1/chat/completions", headers=AUTH, json=chat_body("cache me"))
    second = await client.post("/v1/chat/completions", headers=AUTH, json=chat_body("cache me"))
    bypass = await client.post(
        "/v1/chat/completions",
        headers={**AUTH, "X-Conduit-Cache": "bypass"},
        json=chat_body("cache me"),
    )
    assert first.headers["x-conduit-cache"] == "miss"
    assert second.headers["x-conduit-cache"] == "exact"
    assert bypass.headers["x-conduit-cache"] == "bypass"
    assert first.json()["choices"] == second.json()["choices"]
    assert app.state.runtime.providers["primary"].calls == 2


async def test_semantic_cache_reuses_a_normalized_prompt(tmp_path: Path) -> None:
    async with open_gateway(tmp_path, semantic=True) as (client, app):
        first = await client.post(
            "/v1/chat/completions",
            headers=AUTH,
            json=chat_body("Hello, world. How are you today?"),
        )
        second = await client.post(
            "/v1/chat/completions",
            headers=AUTH,
            json=chat_body("hello world how are you today"),
        )
        assert first.status_code == 200
        assert second.status_code == 200
        assert second.headers["x-conduit-cache"] == "semantic"
        assert app.state.runtime.providers["primary"].calls == 1


async def test_failover_for_timeouts_rate_limits_and_server_errors(gateway) -> None:
    client, app = gateway
    for fault in ("429", "500", "timeout"):
        app.state.runtime.providers["primary"].calls = 0
        app.state.runtime.providers["backup"].calls = 0
        response = await client.post(
            "/v1/chat/completions",
            headers={**AUTH, "X-Conduit-Fault": f"primary={fault}"},
            json=chat_body(fault, model="resilient"),
        )
        assert response.status_code == 200, response.text
        assert response.headers["x-conduit-provider"] == "backup"
        status = {"429": "rate_limited", "500": "server_error", "timeout": "timeout"}[fault]
        assert response.headers["x-conduit-attempt-statuses"] == f"{status},{status},success"
        assert app.state.runtime.providers["primary"].calls == 2
        assert app.state.runtime.providers["backup"].calls == 1


async def test_malformed_stream_fails_over_but_a_midstream_break_does_not(gateway) -> None:
    client, app = gateway
    early = await client.post(
        "/v1/chat/completions",
        headers={**AUTH, "X-Conduit-Fault": "primary=malformed"},
        json=chat_body("early", model="resilient", stream=True),
    )
    assert early.status_code == 200
    assert early.headers["x-conduit-provider"] == "backup"
    assert "conduit-mock:" in _stream_text(early.text)
    backup_calls = app.state.runtime.providers["backup"].calls
    mid = await client.post(
        "/v1/chat/completions",
        headers={**AUTH, "X-Conduit-Fault": "primary=malformed_mid"},
        json=chat_body("mid", model="resilient", stream=True),
    )
    assert mid.status_code == 200
    assert mid.headers["x-conduit-provider"] == "primary"
    assert any(isinstance(event, dict) and "error" in event for event in _events(mid.text))
    assert app.state.runtime.providers["backup"].calls == backup_calls


async def test_circuit_breaker_skips_a_failing_provider(tmp_path: Path) -> None:
    async with open_gateway(tmp_path, failure_threshold=3) as (client, app):
        await _assert_circuit_opens(client, app)


async def _assert_circuit_opens(client, app) -> None:
    fault_headers = {**AUTH, "X-Conduit-Fault": "primary=500", "X-Conduit-Cache": "bypass"}
    for index in range(2):
        response = await client.post(
            "/v1/chat/completions",
            headers=fault_headers,
            json=chat_body(f"trip {index}", model="resilient"),
        )
        assert response.status_code == 200
    calls = app.state.runtime.providers["primary"].calls
    skipped = await client.post(
        "/v1/chat/completions",
        headers=fault_headers,
        json=chat_body("after", model="resilient"),
    )
    assert skipped.status_code == 200
    assert "circuit_open" in skipped.headers["x-conduit-attempt-statuses"]
    assert app.state.runtime.providers["primary"].calls == calls


async def test_upstream_400_is_returned_without_backup(gateway) -> None:
    client, app = gateway
    response = await client.post(
        "/v1/chat/completions",
        headers={**AUTH, "X-Conduit-Fault": "primary=400"},
        json=chat_body("bad", model="resilient"),
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "upstream_rejected"
    assert app.state.runtime.providers["backup"].calls == 0


async def test_unknown_model_and_unsupported_n(gateway) -> None:
    client, _app = gateway
    missing = await client.post("/v1/chat/completions", headers=AUTH, json=chat_body(model="nope"))
    assert missing.status_code == 404
    many = await client.post("/v1/chat/completions", headers=AUTH, json=chat_body(n=2))
    assert many.status_code == 400
    assert many.json()["error"]["code"] == "unsupported_n"


async def test_rate_limits(gateway) -> None:
    client, _app = gateway
    created = await client.post(
        "/admin/keys",
        headers=ADMIN,
        json={"name": "limited", "rpm": 2, "tpm": 1_000_000},
    )
    key = created.json()["api_key"]
    headers = {"Authorization": f"Bearer {key}"}
    assert (await client.post("/v1/chat/completions", headers=headers, json=chat_body("one"))).status_code == 200
    assert (await client.post("/v1/chat/completions", headers=headers, json=chat_body("two"))).status_code == 200
    blocked = await client.post("/v1/chat/completions", headers=headers, json=chat_body("three"))
    assert blocked.status_code == 429
    assert blocked.headers["retry-after"]

    planned = ChatCompletionRequest(
        model="mock",
        messages=[{"role": "user", "content": "token budget please"}],
        max_tokens=7,
    )
    content, prompt, completion = mock_completion(planned, "echo")
    del content
    tight = await client.post(
        "/admin/keys",
        headers=ADMIN,
        json={"name": "tokens", "rpm": 100, "tpm": prompt + completion},
    )
    token_headers = {"Authorization": f"Bearer {tight.json()['api_key']}"}
    body = chat_body("token budget please", max_tokens=7)
    assert (await client.post("/v1/chat/completions", headers=token_headers, json=body)).status_code == 200
    limited = await client.post("/v1/chat/completions", headers=token_headers, json=chat_body("second", max_tokens=7))
    assert limited.status_code == 429


async def test_token_and_cost_budgets_are_hard_limits(gateway) -> None:
    client, _app = gateway
    planned = ChatCompletionRequest(
        model="mock",
        messages=[{"role": "user", "content": "spend"}],
        max_tokens=7,
    )
    _content, prompt, completion = mock_completion(planned, "echo")
    total = prompt + completion
    token_key = await client.post(
        "/admin/keys",
        headers=ADMIN,
        json={"name": "token-cap", "rpm": 100, "tpm": 100000, "token_budget": total},
    )
    headers = {"Authorization": f"Bearer {token_key.json()['api_key']}"}
    body = chat_body("spend", max_tokens=7)
    assert (await client.post("/v1/chat/completions", headers=headers, json=body)).status_code == 200
    blocked = await client.post("/v1/chat/completions", headers=headers, json=body)
    assert blocked.status_code == 402

    cost_key = await client.post(
        "/admin/keys",
        headers=ADMIN,
        json={
            "name": "cost-cap",
            "rpm": 100,
            "tpm": 100000,
            "cost_budget_usd": float(__import__("conduit.costing", fromlist=["micros_to_usd"]).micros_to_usd(total)),
        },
    )
    cost_headers = {"Authorization": f"Bearer {cost_key.json()['api_key']}"}
    assert (await client.post("/v1/chat/completions", headers=cost_headers, json=body)).status_code == 200
    blocked_cost = await client.post(
        "/v1/chat/completions",
        headers={**cost_headers, "X-Conduit-Cache": "bypass"},
        json=chat_body("spend more", max_tokens=7),
    )
    assert blocked_cost.status_code == 402


async def test_idempotent_requests_log_usage_once(gateway) -> None:
    client, app = gateway
    headers = {**AUTH, "Idempotency-Key": "order-42"}
    first = await client.post("/v1/chat/completions", headers=headers, json=chat_body("same"))
    second = await client.post("/v1/chat/completions", headers=headers, json=chat_body("same"))
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert second.headers["x-conduit-idempotent-replay"] == "true"
    assert app.state.runtime.providers["primary"].calls == 1
    usage = await client.get("/admin/usage", headers=ADMIN)
    successes = [row for row in usage.json()["data"] if row["status"] == "success"]
    assert len(successes) == 1
    conflict = await client.post(
        "/v1/chat/completions",
        headers=headers,
        json=chat_body("different"),
    )
    assert conflict.status_code == 409


async def test_concurrent_idempotency_collapses_to_one_upstream_call(gateway) -> None:
    client, app = gateway
    headers = {**AUTH, "Idempotency-Key": "race-1"}
    responses = await asyncio.gather(
        *[client.post("/v1/chat/completions", headers=headers, json=chat_body("race")) for _ in range(6)]
    )
    assert [response.status_code for response in responses] == [200] * 6
    assert len({response.json()["id"] for response in responses}) == 1
    assert app.state.runtime.providers["primary"].calls == 1
    usage = await client.get("/admin/usage?limit=20", headers=ADMIN)
    assert len([row for row in usage.json()["data"] if row["status"] == "success"]) == 1


async def test_disabled_key_and_raw_key_storage(gateway) -> None:
    client, app = gateway
    created = await client.post(
        "/admin/keys",
        headers=ADMIN,
        json={"name": "temp", "rpm": 10, "tpm": 1000},
    )
    raw = created.json()["api_key"]
    key_id = created.json()["id"]
    async with app.state.runtime.sessionmaker() as session:
        row = await session.scalar(select(APIKey).where(APIKey.id == key_id))
        assert row is not None
        assert raw not in row.key_hash
        assert row.key_prefix == raw[:16]
    await client.post(f"/admin/keys/{key_id}/disable", headers=ADMIN)
    rejected = await client.post(
        "/v1/chat/completions",
        headers={"Authorization": f"Bearer {raw}"},
        json=chat_body("nope"),
    )
    assert rejected.status_code == 401


async def test_prompts_and_keys_stay_out_of_logs(gateway, cap_conduit: pytest.LogCaptureFixture) -> None:
    client, _app = gateway
    secret = "SECRET_PROMPT_DO_NOT_LOG_9f3a"
    await client.post("/v1/chat/completions", headers=AUTH, json=chat_body(secret))
    rendered = " ".join(record.getMessage() for record in cap_conduit.records)
    rendered += " ".join(str(getattr(record, "conduit_fields", "")) for record in cap_conduit.records)
    assert secret not in rendered
    assert "cdt_live_test_key" not in rendered


async def test_metrics_expose_the_gateway_series(gateway) -> None:
    client, _app = gateway
    await client.post("/v1/chat/completions", headers=AUTH, json=chat_body("metrics"))
    metrics = await client.get("/metrics")
    assert metrics.status_code == 200
    text = metrics.text
    for name in (
        "conduit_llm_requests_total",
        "conduit_llm_latency_seconds",
        "conduit_llm_time_to_first_token_seconds",
        "conduit_cache_lookups_total",
        "conduit_llm_tokens_total",
        "conduit_http_requests_total",
    ):
        assert name in text


async def test_unconfigured_live_alias_falls_through_and_openai_only_fails(gateway) -> None:
    client, _app = gateway
    live = await client.post("/v1/chat/completions", headers=AUTH, json=chat_body("live", model="live"))
    assert live.status_code == 200
    assert live.headers["x-conduit-attempt-statuses"].startswith("unconfigured")
    down = await client.post("/v1/chat/completions", headers=AUTH, json=chat_body("down", model="openai_only"))
    assert down.status_code == 503
    assert down.json()["error"]["code"] == "all_providers_failed"


async def test_admin_summary_and_rejected_admin_token(gateway) -> None:
    client, _app = gateway
    await client.post("/v1/chat/completions", headers=AUTH, json=chat_body("summary"))
    denied = await client.get("/admin/usage", headers={"X-Admin-Token": "nope"})
    assert denied.status_code == 401
    summary = await client.get("/admin/usage/summary", headers=ADMIN)
    assert summary.status_code == 200
    assert any(row["model"] == "mock" and row["requests"] >= 1 for row in summary.json()["data"])
    listed = await client.get("/admin/keys", headers=ADMIN)
    assert listed.status_code == 200
    assert all("key_hash" not in row and "api_key" not in row for row in listed.json()["data"])


def test_cli_create_and_list(capsys: pytest.CaptureFixture[str]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"id": 7, "api_key": "cdt_live_fromcli", "prefix": "cdt_live_fromcl"})
        return httpx.Response(200, json={"data": []})

    client = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://conduit.test")
    from conduit.cli import main

    assert main(["keys", "create", "--name", "lab", "--rpm", "10"], client=client) == 0
    output = capsys.readouterr().out
    assert "cdt_live_fromcli" in output
    assert main(["keys", "list"], client=client) == 0
    assert main(["usage", "--limit", "5"], client=client) == 0
    client.close()
