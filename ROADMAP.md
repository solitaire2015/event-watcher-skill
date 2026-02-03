# Event Watcher Roadmap

This roadmap is a prioritized list of improvements for the Event Watcher skill.

## P0 — Reliability & Correctness (must-have)
1. **Redis Consumer Groups**
   - Replace XREAD + local cursor with XGROUP / XREADGROUP
   - Proper acking + restart safety
2. **Batch Reads**
   - Support multiple events per read to handle spikes
3. **Explicit Payload Schema**
   - Config: `payloadField`, `payloadEncoding: json|hash|string`
   - Validate required fields; dead-letter on missing
4. **Configurable Event Sources (multi-source)**
   - Add `source` adapters (redis_stream + webhook)
   - Prefer OpenClaw hooks/webhooks for `webhook` source
   - Enforce per-source config blocks

## P1 — Filtering & Observability
5. **Advanced Filter Rules**
   - AND/OR groups, regex, numeric comparisons
6. **Structured Logs & Metrics**
   - received/matched/delivered/failed counters
   - simple backlog length

## P2 — Delivery & Ops
7. **Rate Limiting / Grouping**
   - Aggregate bursts into one notification
8. **Healthcheck + Graceful Shutdown**
   - Redis connectivity + stream existence checks
   - SIGTERM flush state
9. **Dead-letter Replay Tooling**
   - Inspect and replay failed events

## P3 — Convenience
10. **Natural-language → Filter Rules Helper**
    - Optional agent tool to convert NL into JSON rules
11. **pm2 ecosystem.config.js**
    - Standardized launch config
