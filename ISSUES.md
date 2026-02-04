# Event Watcher Issues

## P0
- [x] Use Redis consumer groups (XGROUP/XREADGROUP) for at-least-once delivery
- [x] Implement batch reads (count > 1) and backlog draining
- [x] Add explicit payload parsing config (`payloadField`, `payloadEncoding`)
- [x] Add multi-source adapter framework (redis_stream + webhook)
- [x] Implement webhook source via OpenClaw hooks/webhooks

## P1
- [x] Support AND/OR filter groups + regex operations
- [x] Add structured logs + counters (received/matched/delivered/failed)

## P2
- [ ] Add Kafka adapter
- [ ] Add SQS adapter
- [ ] Support alternative daemons (systemd/launchd/supervisord)
- [ ] Rate-limit / aggregate burst notifications
- [ ] Add healthcheck + graceful shutdown
- [ ] Dead-letter replay CLI

## P3
- [ ] Natural language → filter rules helper
- [ ] pm2 ecosystem.config.js generator
