#!/usr/bin/env bash
set -euo pipefail
pm2 stop event-watcher || true
