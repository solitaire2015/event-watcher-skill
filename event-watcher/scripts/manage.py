#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import yaml

DEFAULT_CONFIG = os.environ.get("EVENT_WATCHER_CONFIG", "event_watcher.yaml")


def load_config(path: str) -> dict:
    if not os.path.exists(path):
        return {"watchers": []}
    with open(path, "r") as f:
        return yaml.safe_load(f) or {"watchers": []}


def save_config(path: str, data: dict) -> None:
    with open(path, "w") as f:
        yaml.safe_dump(data, f, sort_keys=False)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=DEFAULT_CONFIG)
    sub = ap.add_subparsers(dest="cmd", required=True)

    add = sub.add_parser("add")
    add.add_argument("--name", required=True)
    add.add_argument("--stream", required=True)
    add.add_argument("--session-key", required=True)
    add.add_argument("--filter", default=None, help='JSON-ish filter string, e.g. field=payload.weather op!= value=sunny')

    rm = sub.add_parser("remove")
    rm.add_argument("--name", required=True)

    ls = sub.add_parser("list")

    args = ap.parse_args()
    cfg = load_config(args.config)

    if args.cmd == "list":
        for w in cfg.get("watchers", []):
            print(w.get("name"))
        return

    if args.cmd == "remove":
        cfg["watchers"] = [w for w in cfg.get("watchers", []) if w.get("name") != args.name]
        save_config(args.config, cfg)
        return

    if args.cmd == "add":
        watcher = {
            "name": args.name,
            "source": "redis_stream",
            "stream": args.stream,
            "filter": None,
            "dedupe_ttl_seconds": 1800,
            "ack_timeout_seconds": 30,
            "retry": {"max": 3, "backoff_seconds": [60, 300, 900]},
            "wake": {"method": "sessions_send", "session_key": args.session_key, "message_template": "New event: {{event_id}}"},
        }
        cfg.setdefault("watchers", []).append(watcher)
        save_config(args.config, cfg)


if __name__ == "__main__":
    main()
