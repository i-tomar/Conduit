"""Server-rendered home and admin pages. No frontend build."""

from __future__ import annotations

from conduit import __version__
from conduit.config_loader import GatewayConfig

_CSS = """
:root {
  color-scheme: light;
  --paper: #e7e1d5;
  --card: #f6f3ec;
  --ink: #1c1915;
  --muted: #5f584e;
  --line: #d3cbbd;
  --copper: #c24d1d;
  --good: #1f6b45;
  --bad: #9d2c2c;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--paper);
  color: var(--ink);
  font-family: "Iowan Old Style", Palatino, "Palatino Linotype", Georgia, serif;
}
a { color: var(--copper); }
header, main { width: min(1080px, calc(100% - 32px)); margin: 0 auto; }
header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 16px;
  padding: 28px 0 18px;
  border-bottom: 1px solid var(--line);
}
.mark { font-size: 13px; letter-spacing: 0.18em; text-transform: uppercase; }
h1 { font-weight: 500; font-size: 42px; line-height: 1.05; margin: 18px 0 8px; }
.lede { font-size: 18px; max-width: 62ch; color: var(--muted); }
.grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; margin: 28px 0; }
.card, .panel {
  background: var(--card);
  border: 1px solid var(--line);
  padding: 16px 18px;
}
.card h2, .panel h2 { margin: 0 0 8px; font-size: 16px; font-weight: 600; }
.mono, code, pre, input, button, table { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
pre {
  background: #231f1b;
  color: #f4efe6;
  padding: 14px;
  overflow: auto;
  font-size: 13px;
}
.pill { border: 1px solid var(--line); padding: 2px 8px; font-size: 12px; }
.ok { color: var(--good); }
.bad { color: var(--bad); }
form { display: grid; gap: 8px; }
label { display: grid; gap: 4px; font-size: 13px; color: var(--muted); }
input {
  border: 1px solid var(--line);
  background: white;
  color: var(--ink);
  padding: 8px;
  font-size: 14px;
}
button {
  background: var(--ink);
  color: var(--paper);
  border: 0;
  padding: 8px 12px;
  cursor: pointer;
}
button.ghost { background: transparent; color: var(--ink); border: 1px solid var(--line); }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { text-align: left; padding: 8px 6px; border-bottom: 1px solid var(--line); vertical-align: top; }
.secret { border: 1px dashed var(--copper); padding: 12px; background: #fff8f3; word-break: break-all; }
.layout { display: grid; grid-template-columns: 280px 1fr; gap: 16px; margin: 22px 0 48px; }
.stack { display: grid; gap: 16px; }
@media (max-width: 800px) {
  h1 { font-size: 32px; }
  .grid, .layout { grid-template-columns: 1fr; }
  header { flex-direction: column; }
  table { display: block; overflow-x: auto; }
}
"""


def home_page(config: GatewayConfig) -> str:
    aliases = "".join(
        f"<li><code>{name}</code> → "
        + " → ".join(f"{target.provider}/{target.model}" for target in spec.targets)
        + "</li>"
        for name, spec in config.aliases.items()
    )
    semantic = "on" if config.cache.semantic_enabled else "off"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Conduit</title>
  <style>{_CSS}</style>
</head>
<body>
  <header>
    <div>
      <div class="mark">Conduit {__version__}</div>
      <h1>An OpenAI-compatible gateway for several model providers.</h1>
      <p class="lede">Point any OpenAI SDK at this base URL. Conduit routes the call, fails over when a provider times out or errors, and records the tokens.</p>
    </div>
    <div class="pill">health <span id="health">checking</span></div>
  </header>
  <main>
    <section class="grid">
      <article class="card"><h2>Route</h2><p>Aliases map to an ordered list of provider and model targets. Timeouts, 429s, and 5xx move to the next target. A circuit breaker stops a provider that keeps failing.</p></article>
      <article class="card"><h2>Cache</h2><p>Exact matches are cached per tenant. Semantic cache is {semantic}. Tool calls are not cached.</p></article>
      <article class="card"><h2>Account</h2><p>Each gateway key has a request rate, a token rate, and optional hard token and cost budgets. Usage is written once per request.</p></article>
    </section>
    <section class="panel">
      <h2>Try the mock alias</h2>
      <pre>curl http://localhost:8091/v1/chat/completions \\
  -H "Authorization: Bearer $CONDUIT_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{{"model":"mock","messages":[{{"role":"user","content":"hello"}}]}}'</pre>
      <p>Configured aliases:</p>
      <ul>{aliases}</ul>
      <p><a href="/docs">API docs</a> · <a href="/admin">Admin</a> · <a href="/metrics">Metrics</a></p>
    </section>
  </main>
  <script>
    fetch("/health").then(function (response) {{ return response.json(); }}).then(function (body) {{
      var node = document.getElementById("health");
      node.textContent = body.status;
      node.className = body.status === "ok" ? "ok" : "bad";
    }}).catch(function () {{
      document.getElementById("health").textContent = "unreachable";
    }});
  </script>
</body>
</html>"""


def admin_page() -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Conduit admin</title>
  <style>{_CSS}</style>
</head>
<body>
  <header>
    <div>
      <div class="mark">Conduit admin</div>
      <h1>Keys and usage</h1>
      <p class="lede">The admin token stays in this browser tab. A raw API key is shown once, when it is created.</p>
    </div>
    <a href="/">Gateway</a>
  </header>
  <main class="layout">
    <section class="stack">
      <form id="gate" class="panel">
        <h2>Admin token</h2>
        <label>X-Admin-Token<input id="token" type="password" autocomplete="current-password"></label>
        <button type="submit">Save token</button>
        <p id="gate-note"></p>
      </form>
      <form id="create" class="panel">
        <h2>New key</h2>
        <label>Name<input id="name" required value="local"></label>
        <label>Requests / minute<input id="rpm" type="number" min="1" value="60"></label>
        <label>Tokens / minute<input id="tpm" type="number" min="1" value="60000"></label>
        <label>Token budget (optional)<input id="token-budget" type="number" min="1"></label>
        <label>Cost budget USD (optional)<input id="cost-budget" type="number" min="0" step="0.000001"></label>
        <button type="submit">Create key</button>
      </form>
    </section>
    <section class="stack">
      <div id="secret" class="secret" hidden></div>
      <div class="panel">
        <h2>Keys</h2>
        <table><thead><tr><th>Name</th><th>Prefix</th><th>Limits</th><th>Used</th><th></th></tr></thead><tbody id="keys"></tbody></table>
      </div>
      <div class="panel">
        <h2>Recent usage</h2>
        <table><thead><tr><th>When</th><th>Model</th><th>Provider</th><th>Tokens</th><th>Cost</th><th>Cache</th><th>Status</th></tr></thead><tbody id="usage"></tbody></table>
      </div>
    </section>
  </main>
  <script>
    var storageKey = "conduit_admin_token";
    function authHeaders() {{
      return {{ "X-Admin-Token": sessionStorage.getItem(storageKey) || "", "Content-Type": "application/json" }};
    }}
    function cell(text) {{
      var node = document.createElement("td");
      node.textContent = text;
      return node;
    }}
    async function load() {{
      var token = sessionStorage.getItem(storageKey) || "";
      document.getElementById("token").value = token;
      if (!token) return;
      var keysResponse = await fetch("/admin/keys", {{ headers: authHeaders() }});
      if (keysResponse.status === 401) {{
        document.getElementById("gate-note").textContent = "That admin token was rejected.";
        return;
      }}
      document.getElementById("gate-note").textContent = "Token accepted for this tab.";
      var keys = await keysResponse.json();
      var keyBody = document.getElementById("keys");
      keyBody.replaceChildren();
      keys.data.forEach(function (key) {{
        var row = document.createElement("tr");
        row.append(cell(key.name), cell(key.prefix), cell(key.rpm + " rpm / " + key.tpm + " tpm"), cell(String(key.tokens_used)));
        var action = document.createElement("td");
        if (!key.disabled) {{
          var button = document.createElement("button");
          button.className = "ghost";
          button.textContent = "Disable";
          button.addEventListener("click", async function () {{
            await fetch("/admin/keys/" + key.id + "/disable", {{ method: "POST", headers: authHeaders() }});
            load();
          }});
          action.append(button);
        }} else {{
          action.textContent = "disabled";
        }}
        row.append(action);
        keyBody.append(row);
      }});
      var usageResponse = await fetch("/admin/usage?limit=20", {{ headers: authHeaders() }});
      var usage = await usageResponse.json();
      var usageBody = document.getElementById("usage");
      usageBody.replaceChildren();
      usage.data.forEach(function (item) {{
        var row = document.createElement("tr");
        row.append(
          cell(item.created_at),
          cell(item.model),
          cell(item.provider || ""),
          cell(String(item.total_tokens)),
          cell(item.cost_usd),
          cell(item.cache),
          cell(item.status)
        );
        usageBody.append(row);
      }});
    }}
    document.getElementById("gate").addEventListener("submit", function (event) {{
      event.preventDefault();
      sessionStorage.setItem(storageKey, document.getElementById("token").value.trim());
      load();
    }});
    document.getElementById("create").addEventListener("submit", async function (event) {{
      event.preventDefault();
      var payload = {{
        name: document.getElementById("name").value,
        rpm: Number(document.getElementById("rpm").value),
        tpm: Number(document.getElementById("tpm").value)
      }};
      var tokenBudget = document.getElementById("token-budget").value;
      var costBudget = document.getElementById("cost-budget").value;
      if (tokenBudget) payload.token_budget = Number(tokenBudget);
      if (costBudget) payload.cost_budget_usd = Number(costBudget);
      var response = await fetch("/admin/keys", {{ method: "POST", headers: authHeaders(), body: JSON.stringify(payload) }});
      var body = await response.json();
      var box = document.getElementById("secret");
      if (!response.ok) {{
        box.hidden = false;
        box.textContent = (body.error && body.error.message) || "Could not create the key";
        return;
      }}
      box.hidden = false;
      box.textContent = "Copy this key now. It will not be shown again: " + body.api_key;
      load();
    }});
    load();
  </script>
</body>
</html>"""
