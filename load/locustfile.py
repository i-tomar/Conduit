"""Load the gateway with Locust.

    CONDUIT_API_KEY=cdt_live_dev_local_key \
    locust -f load/locustfile.py --host http://127.0.0.1:8091
"""

from __future__ import annotations

import os

from locust import HttpUser, between, task


class ChatUser(HttpUser):
    wait_time = between(0.01, 0.05)

    def on_start(self) -> None:
        token = os.environ.get("CONDUIT_API_KEY", "cdt_live_dev_local_key")
        self.client.headers["Authorization"] = f"Bearer {token}"

    @task(5)
    def chat(self) -> None:
        model = os.environ.get("CONDUIT_MODEL", "mock")
        self.client.post(
            "/v1/chat/completions",
            json={
                "model": model,
                "messages": [{"role": "user", "content": "Say ok."}],
                "max_tokens": 16,
            },
            name="/v1/chat/completions",
        )

    @task(1)
    def models(self) -> None:
        self.client.get("/v1/models", name="/v1/models")
