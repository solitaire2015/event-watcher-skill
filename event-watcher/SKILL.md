---
name: event-watcher
description: Event watcher skill for OpenClaw. Use when you need a daemon that subscribes to event sources (Redis Streams + webhook JSONL) and wakes an agent only when matching events arrive. Covers filtering, dedupe, retry, and session routing via sessions_send/agent_gate.
---

# Event Watcher

## Overview
Lightweight event watcher daemon that listens to Redis Streams (and webhook JSONL) and wakes an OpenClaw session only on matching events. No events → no agent wake → no token spend.

## Core Capabilities
1. **Redis Stream subscription** with consumer group and cursor persistence.
2. **Webhook JSONL ingestion** via `webhook_bridge.py`.
3. **Filtering** via JSON rules (supports AND/OR + regex).
4. **Deduplication** with TTL (configurable).
5. **Retry** on failed delivery.
6. **Session routing** via `sessions_send` or `agent_gate`.
7. **Structured logging + counters** for received/matched/delivered/failed.

## Workflow (MVP)
1. Read watcher config (YAML) from `references/CONFIG.md`.
2. Run the watcher (see examples).
3. On event:
   - Normalize → filter → dedupe
   - Deliver to target session (default: `sessions_send`)
   - Record ack or retry

## Scripts
- `scripts/watcher.py` — multi-source watcher (redis_stream, webhook)
- `scripts/webhook_bridge.py` — append webhook payloads to JSONL
- `scripts/manage.py` — add/update/remove/list watcher configs
- `scripts/requirements.txt` — Python deps (redis, pyyaml)

## Daemon Templates
- `daemon/systemd/event-watcher.service`
- `daemon/launchd/com.openclaw.event-watcher.plist`
- `daemon/env.example`

## References
- See `references/CONFIG.md` for full configuration spec, examples, and routing rules.
