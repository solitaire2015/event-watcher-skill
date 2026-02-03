---
name: event-watcher
description: Event watcher skill for OpenClaw. Use when you need to design, configure, or implement a daemon that subscribes to event sources (starting with Redis Streams) and wakes an agent via sessions_send only when matching events arrive. Covers pm2-based daemon management, event filtering, dedupe, retry/dead-letter handling, and session routing.
---

# Event Watcher

## Overview
Design and operate a lightweight event watcher daemon (pm2-managed) that listens to Redis Streams and wakes an OpenClaw session only on matching events. No events → no agent wake → no token spend.

## Core Capabilities
1. **Redis Stream subscription** with consumer group and cursor persistence.
2. **Event normalization** to a unified schema.
3. **Filtering** via JSON rules (supports AND/OR + regex).
4. **Deduplication** with TTL (configurable).
5. **Retry + dead-letter** on failed delivery.
6. **Session routing** via `sessions_send` (configurable per watcher).
7. **Structured logging + counters** for received/matched/delivered/failed.
8. **Daemon management** using pm2 (start/stop/logs).

## Workflow (MVP)
1. Read watcher config (YAML) from `references/CONFIG.md`.
2. Start daemon with pm2 (scripts below).
3. On event:
   - Normalize → filter → dedupe
   - Send to target session with `sessions_send`
   - Record ack or retry
   - Write dead_letter.jsonl on terminal failure

## Scripts
- `scripts/watcher.py` — multi-source watcher (redis_stream, webhook)
- `scripts/webhook_bridge.py` — append webhook payloads to JSONL
- `scripts/manage.py` — add/update/remove/list watcher configs
- `scripts/pm2_start.sh` — start daemon under pm2
- `scripts/pm2_stop.sh` — stop daemon
- `scripts/requirements.txt` — Python deps (redis, pyyaml)

## References
- See `references/CONFIG.md` for full configuration spec, examples, and routing rules.
