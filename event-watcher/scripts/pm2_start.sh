#!/usr/bin/env bash
set -euo pipefail

CONFIG=${EVENT_WATCHER_CONFIG:-event_watcher.yaml}
STATE=${EVENT_WATCHER_STATE:-event_watcher_state.json}

pm2 start ./scripts/watcher.py --name event-watcher -- --config "$CONFIG" --state "$STATE"
pm2 save
