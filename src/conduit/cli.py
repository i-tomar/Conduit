"""Small admin CLI. The gateway process is started with uvicorn."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

import httpx


def main(argv: list[str] | None = None, client: httpx.Client | None = None) -> int:
    parser = argparse.ArgumentParser(prog="conduit", description="Talk to a running Conduit admin API")
    parser.add_argument("--url", default=os.environ.get("CONDUIT_URL", "http://127.0.0.1:8091"))
    sub = parser.add_subparsers(dest="command", required=True)

    keys = sub.add_parser("keys", help="Create and list gateway API keys")
    key_commands = keys.add_subparsers(dest="keys_command", required=True)
    create = key_commands.add_parser("create")
    create.add_argument("--name", required=True)
    create.add_argument("--rpm", type=int, default=60)
    create.add_argument("--tpm", type=int, default=60_000)
    create.add_argument("--token-budget", type=int)
    create.add_argument("--cost-budget", type=float)
    key_commands.add_parser("list")

    usage = sub.add_parser("usage", help="Show recent usage rows")
    usage.add_argument("--limit", type=int, default=20)

    args = parser.parse_args(argv)
    owns_client = client is None
    http = client or httpx.Client(base_url=args.url, timeout=30)
    try:
        token = os.environ.get("CONDUIT_ADMIN_TOKEN", "")
        headers = {"X-Admin-Token": token}
        if args.command == "keys" and args.keys_command == "create":
            payload: dict[str, Any] = {"name": args.name, "rpm": args.rpm, "tpm": args.tpm}
            if args.token_budget is not None:
                payload["token_budget"] = args.token_budget
            if args.cost_budget is not None:
                payload["cost_budget_usd"] = args.cost_budget
            response = http.post("/admin/keys", headers=headers, json=payload)
        elif args.command == "keys":
            response = http.get("/admin/keys", headers=headers)
        else:
            response = http.get("/admin/usage", headers=headers, params={"limit": args.limit})
        if response.status_code >= 400:
            print(response.text)
            return 1
        print(json.dumps(response.json(), indent=2))
        return 0
    finally:
        if owns_client:
            http.close()
