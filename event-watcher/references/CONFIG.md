# Event Watcher v0 Config Spec (Redis Streams + pm2)

## Goals
- No events → no agent wake → no token cost
- Events trigger agent via `sessions_send`
- Multiple watchers; each watcher can target a different session

---

## Environment Variables (Auth)
Use env vars for Redis auth (first version):

```
REDIS_URL=redis://:password@host:6379/0
# or
REDIS_HOST=...
REDIS_PORT=...
REDIS_PASSWORD=...
```

Optional:
```
OPENCLAW_SESSION_KEY=...   # default session for wake if watcher omits it
```

---

## Unified Event Schema
All sources normalized to:
```json
{
  "event_id": "...",
  "source": "redis_stream",
  "topic": "weather_events",
  "timestamp": "2026-02-03T12:00:00Z",
  "payload": {"weather": "rain"}
}
```

---

## YAML Config
```yaml
watchers:
  - name: weather_stream
    source: redis_stream
    stream: weather_events
    group: eventwatcher
    consumer: watcher-1
    batch_count: 10
    block_ms: 1000
    payloadField: payload
    payloadEncoding: json
    filter:
      field: "payload.weather"
      op: "!="
      value: "sunny"
    dedupe_ttl_seconds: 1800
    ack_timeout_seconds: 30
    retry:
      max: 3
      backoff_seconds: [60, 300, 900]
    wake:
      method: sessions_send
      session_key: "<openclaw_session_key>"
      message_template: |
        New event: {{event_id}}
```

### Webhook Source (via OpenClaw hooks)
```yaml
watchers:
  - name: webhook_events
    source: webhook
    webhook_log_path: /root/.openclaw/workspace/webhook_events.jsonl
    batch_count: 50
    filter:
      field: "payload.type"
      op: "!="
      value: "sunny"
    wake:
      method: sessions_send
      session_key: "<openclaw_session_key>"
      message_template: "Webhook event {{event_id}}"
```

Use `scripts/webhook_bridge.py` as the hook target to append incoming payloads to the JSONL file.

### Fields
- `name`: unique watcher id
- `source`: `redis_stream | webhook | sqs | kafka`
- `stream`: Redis stream name (redis_stream)
- `group`, `consumer`: Redis consumer group settings (redis_stream)
- `batch_count`: number of events per read (default 10)
- `block_ms`: Redis block time (default 1000) (redis_stream)
- `payloadField`: field to parse as payload (redis_stream)
- `payloadEncoding`: `json|hash|string` (redis_stream)
- `webhook_log_path`: JSONL file path (webhook)
- `filter`: JSON rule (see below)
- `dedupe_ttl_seconds`: default 1800
- `ack_timeout_seconds`: default 30
- `retry`: max + backoff list
- `wake.method`: `sessions_send` (implemented via `openclaw agent --session-id`)
- `wake.session_key`: target session id (from `openclaw sessions --json`)
- `wake.message_template`: text for the agent

---

## Filter Rules
Simple rule:
```json
{"field":"payload.weather","op":"!=","value":"sunny"}
```

Group rules:
```json
{"all":[
  {"field":"payload.type","op":"!=","value":"sunny"},
  {"any":[
    {"field":"payload.city","op":"==","value":"beijing"},
    {"field":"payload.city","op":"==","value":"shanghai"}
  ]}
]}
```

Supported `op`: `==`, `!=`, `>`, `<`, `>=`, `<=`, `in`, `contains`, `regex`

Natural language can be translated by the agent into JSON.

---

## Deduplication
- store `event_id` in Redis key
- default TTL: 1800s (configurable)

---

## Retry + Dead Letter
- if `sessions_send` fails or ack timeout:
  - retry with backoff
  - after max attempts, append to `dead_letter.jsonl`

`dead_letter.jsonl` entry:
```json
{"event_id":"...","reason":"ack_timeout","last_attempt":"...","payload":{...}}
```

---

## Session Routing
- Each watcher can target a session via `wake.session_key`
- If omitted, use env `OPENCLAW_SESSION_KEY`
- The agent decides whether to notify the user

---

## Logging + Metrics
- Structured event logs: `EVENT_WATCHER_LOG` (default `event_watcher_events.jsonl`)
- State file includes counters per watcher: received/matched/delivered/failed/filtered/deduped

## pm2 Management (MVP)
- Start: `./scripts/pm2_start.sh`
- Stop: `./scripts/pm2_stop.sh`
- Logs: `pm2 logs event-watcher`

---

## Future Extensions
- Add `source: sqs | kafka | webhook`
- Add per-watcher rate limits and quiet hours
