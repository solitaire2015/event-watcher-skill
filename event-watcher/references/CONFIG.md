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

### Fields
- `name`: unique watcher id
- `source`: currently only `redis_stream`
- `stream`: Redis stream name
- `group`, `consumer`: Redis consumer group settings
- `filter`: JSON rule (see below)
- `dedupe_ttl_seconds`: default 1800
- `ack_timeout_seconds`: default 30
- `retry`: max + backoff list
- `wake.method`: `sessions_send`
- `wake.session_key`: target session
- `wake.message_template`: text for the agent

---

## Filter Rules
JSON rule format:
```json
{"field":"payload.weather","op":"!=","value":"sunny"}
```
Supported `op`: `==`, `!=`, `>`, `<`, `in`, `contains`

Natural language can be translated by the agent into JSON.

---

## Deduplication
- store `event_id` in in-memory LRU or Redis key
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

## pm2 Management (MVP)
- Start: `pm2 start scripts/watcher.py --name event-watcher`
- Stop: `pm2 stop event-watcher`
- Logs: `pm2 logs event-watcher`

---

## Future Extensions
- Add `source: sqs | kafka | webhook`
- Add per-watcher rate limits and quiet hours
