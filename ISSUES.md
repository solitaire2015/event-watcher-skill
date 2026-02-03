# Event Watcher Issues

## P0
- [ ] Use Redis consumer groups (XGROUP/XREADGROUP) for at-least-once delivery
- [ ] Implement batch reads (count > 1) and backlog draining
- [ ] Add explicit payload parsing config (`payloadField`, `payloadEncoding`)
- [ ] Add multi-source adapter framework (redis_stream + webhook)
- [ ] Implement webhook source via OpenClaw hooks/webhooks

## P1
- [ ] Support AND/OR filter groups + regex operations
- [ ] Add structured logs + counters (received/matched/delivered/failed)

## P2
- [ ] Add Kafka adapter
- [ ] Add SQS adapter
- [ ] Rate-limit / aggregate burst notifications
- [ ] Add healthcheck + graceful shutdown
- [ ] Dead-letter replay CLI

## P3
- [ ] Natural language → filter rules helper
- [ ] pm2 ecosystem.config.js generator
