# Event Watcher Design

This document describes the agreed implementation approach based on the requirements.

## Goals (summary)
- Route events into the **target channel’s session** to preserve context.
- Support both **lightweight single‑turn** and **complex multi‑turn** flows.
- Keep the watcher simple and reliable; avoid deep reasoning loops in the watcher.

---

## 1) Delivery Modes

### A. `sessions_send + deliver` (default for complex tasks)
**Use case:** multi‑turn tool workflows (e.g., GH issue triage).

Flow:
1. Event passes filters.
2. Watcher calls `openclaw agent --session-id ... --message ... --deliver --reply-channel ... --reply-to ...`.
3. Agent runs inside the **channel session** and replies directly to that channel.

Why:
- Agent can do multi‑turn tool calls within its session.
- User can continue the conversation in the same channel.

### B. `agent_gate` (single‑turn)
**Use case:** lightweight decisions (e.g., "notify or ignore").

Flow:
1. Event passes filters.
2. Watcher runs one agent turn.
3. If output is `NO_REPLY` → suppress; otherwise deliver.

Why:
- Fast and low cost.
- Good for simple yes/no decision events.

---

## 2) Session Selection Strategy

- **Default:** resolve the **latest session** for the target channel.
- **Optional override:** `session_id` can be explicitly configured per watcher.

Rationale:
- Latest session enables `/new` flows without manual updates.
- Explicit override supports fixed, long‑lived sessions when desired.

---

## 3) Channel Targeting

Each watcher should configure:
- `reply_channel` (e.g., `slack`)
- `reply_to` (e.g., `channel:C0ACE3NL61M` or `user:U0AB1EWFVFW`)

These are used for direct delivery with `--deliver`.

---

## 4) Failure Handling (initial scope)

- The watcher **records errors** from the CLI call (exit code/stderr).
- No multi‑turn retry logic inside watcher.
- Dead‑letter is used only for delivery failures or missing config.

Future option (not in scope now): integrate spool or gateway‑side execution for higher reliability.

---

## 5) Prompt Size & Guidance

- Support `prompt_file` (short message pointing to a guide file).
- Use in `wake` config to avoid sending large prompts repeatedly.

---

## 6) Safety & Attribution

- Event prompts should include `source/topic` attribution to avoid prompt‑injection confusion.

---

## Configuration Sketch (example)
```yaml
watchers:
  - name: issue_triage
    source: webhook
    webhook_log_path: /root/.openclaw/workspace/webhook_events.jsonl
    filter:
      field: "payload.type"
      op: "=="
      value: "issue"
    wake:
      method: sessions_send
      session_id: "<optional override>"
      reply_channel: slack
      reply_to: channel:C0ACE3NL61M
      prompt_file: prompts/issue_triage.md
```

---

Last updated: 2026-02-06
