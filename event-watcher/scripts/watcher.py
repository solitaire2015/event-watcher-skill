#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from typing import Any, Dict

import yaml
import redis

DEFAULT_CONFIG = os.environ.get("EVENT_WATCHER_CONFIG", "event_watcher.yaml")
DEFAULT_STATE = os.environ.get("EVENT_WATCHER_STATE", "event_watcher_state.json")
DEAD_LETTER = os.environ.get("EVENT_WATCHER_DEAD_LETTER", "dead_letter.jsonl")
OPENCLAW_SESSION_KEY = os.environ.get("OPENCLAW_SESSION_KEY")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_config(path: str) -> dict:
    if not os.path.exists(path):
        raise SystemExit(f"Config not found: {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def load_state(path: str) -> dict:
    if not os.path.exists(path):
        return {"cursors": {}, "attempts": {}}
    with open(path, "r") as f:
        return json.load(f)


def save_state(path: str, state: dict) -> None:
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def normalize_event(stream: str, event_id: str, fields: Dict[str, Any]) -> dict:
    # decode bytes to str
    payload = {k.decode() if isinstance(k, bytes) else k: (v.decode() if isinstance(v, bytes) else v) for k, v in fields.items()}

    # Common pattern: a single field named "payload" that contains JSON.
    # If present, parse it into an object so filters can use payload.type, payload.city, etc.
    if isinstance(payload.get("payload"), str):
        try:
            parsed = json.loads(payload["payload"])
            if isinstance(parsed, dict):
                payload = parsed
        except Exception:
            pass

    return {
        "event_id": event_id,
        "source": "redis_stream",
        "topic": stream,
        "timestamp": utc_now(),
        "payload": payload,
    }


def get_field(data: dict, path: str) -> Any:
    cur = data
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def match_filter(event: dict, rule: dict | None) -> bool:
    if not rule:
        return True
    field = rule.get("field")
    op = rule.get("op")
    value = rule.get("value")
    actual = get_field(event, field) if field else None
    if op == "==":
        return actual == value
    if op == "!=":
        return actual != value
    if op == ">":
        return actual is not None and actual > value
    if op == "<":
        return actual is not None and actual < value
    if op == "in":
        return actual in value if isinstance(value, (list, tuple, set)) else False
    if op == "contains":
        return value in actual if isinstance(actual, (list, str)) else False
    return False


_TPL_RE = re.compile(r"\{\{\s*([^\}]+?)\s*\}\}")


def render_template(template: str, event: dict) -> str:
    """Very small templater.

    Supports:
      - {{event_id}}, {{topic}}, {{timestamp}}, {{payload}}
      - nested paths like {{payload.city}} (dot-separated)
    """
    if not template:
        return f"New event: {event['event_id']}"

    def repl(m: re.Match) -> str:
        path = m.group(1)
        val = get_field(event, path)
        return "" if val is None else str(val)

    return _TPL_RE.sub(repl, template)


def _extract_reply(payload: dict, fallback: str) -> str:
    # Best-effort extraction for openclaw --json output
    if not isinstance(payload, dict):
        return fallback
    for path in (
        ["result", "reply"],
        ["result", "message"],
        ["result", "text"],
        ["reply"],
        ["message"],
        ["text"],
        ["output"],
        ["content"],
    ):
        cur = payload
        ok = True
        for key in path:
            if isinstance(cur, dict) and key in cur:
                cur = cur[key]
            else:
                ok = False
                break
        if ok and isinstance(cur, str):
            return cur
    return fallback


def _parse_json_stdout(stdout: str) -> dict | None:
    if not stdout:
        return None
    # First try whole stdout (some versions emit compact JSON)
    try:
        return json.loads(stdout)
    except Exception:
        pass

    # Then try to find the last JSON object in the output (handles pretty JSON + noise)
    text = stdout
    idx = text.rfind("{")
    while idx != -1:
        try:
            return json.loads(text[idx:])
        except Exception:
            idx = text.rfind("{", 0, idx)
    return None


def run_agent(
    session_key: str,
    message: str,
    timeout: int,
    deliver: bool = False,
    reply_channel: str | None = None,
    reply_to: str | None = None,
) -> tuple[bool, str, dict]:
    """Run an OpenClaw agent turn for a specific session.

    Returns (ok, reply_text, debug). If deliver=True, the agent's reply will be sent
    to the selected channel/target by the CLI; reply_text may be empty.
    """
    cmd = [
        "openclaw",
        "agent",
        "--session-id",
        session_key,
        "--message",
        message,
        "--timeout",
        str(timeout),
        "--json",
    ]
    if deliver:
        cmd.append("--deliver")
        if reply_channel:
            cmd += ["--reply-channel", reply_channel]
        if reply_to:
            cmd += ["--reply-to", reply_to]

    try:
        proc = subprocess.run(
            cmd,
            check=True,
            timeout=timeout + 10,
            capture_output=True,
            text=True,
        )
        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        reply = ""
        if stdout:
            payload = _parse_json_stdout(stdout)
            if payload is not None:
                reply = _extract_reply(payload, "")
        debug = {
            "stdout": stdout[-2000:] if stdout else "",
            "stderr": stderr[-2000:] if stderr else "",
        }
        return True, (reply or ""), debug
    except Exception as e:
        return False, "", {"error": str(e)}


def send_message(channel: str, target: str, message: str, timeout: int) -> bool:
    """Send a direct message via the Clawdbot CLI (no agent turn)."""
    cmd = [
        "openclaw",
        "message",
        "send",
        "--channel",
        channel,
        "--target",
        target,
        "--message",
        message,
        "--json",
    ]
    try:
        subprocess.run(cmd, check=True, timeout=timeout + 10)
        return True
    except Exception:
        return False


def append_jsonl(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def append_dead_letter(event: dict, reason: str) -> None:
    entry = {
        "event_id": event.get("event_id"),
        "reason": reason,
        "last_attempt": utc_now(),
        "payload": event,
    }
    with open(DEAD_LETTER, "a") as f:
        f.write(json.dumps(entry) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=DEFAULT_CONFIG)
    ap.add_argument("--state", default=DEFAULT_STATE)
    ap.add_argument("--poll-interval", type=float, default=1.0)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.config)
    watchers = cfg.get("watchers", [])
    if not watchers:
        raise SystemExit("No watchers defined in config")

    state = load_state(args.state)
    r = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"))

    while True:
        any_work = False
        for w in watchers:
            name = w.get("name")
            stream = w.get("stream")
            if not stream:
                continue

            last_id = state["cursors"].get(name, "0-0")
            resp = r.xread({stream: last_id}, count=1, block=1000)
            if not resp:
                continue
            any_work = True

            _, entries = resp[0]
            for event_id, fields in entries:
                event_id = event_id.decode() if isinstance(event_id, bytes) else event_id
                event = normalize_event(stream, event_id, fields)

                # filter
                if not match_filter(event, w.get("filter")):
                    state["cursors"][name] = event_id
                    save_state(args.state, state)
                    continue

                # dedupe using redis key
                ttl = int(w.get("dedupe_ttl_seconds", 1800))
                dedupe_key = f"eventwatcher:dedupe:{stream}:{event_id}"
                if not r.set(dedupe_key, "1", nx=True, ex=ttl):
                    state["cursors"][name] = event_id
                    save_state(args.state, state)
                    continue

                # deliver
                wake = w.get("wake", {})
                method = wake.get("method", "sessions_send")

                message = render_template(wake.get("message_template", ""), event)
                timeout = int(w.get("ack_timeout_seconds", 30))

                # optional per-event logging when matched
                log_path = wake.get("log_path")
                if log_path:
                    append_jsonl(
                        log_path,
                        {
                            "event_id": event.get("event_id"),
                            "timestamp": event.get("timestamp"),
                            "topic": event.get("topic"),
                            "payload": event.get("payload"),
                        },
                    )

                ok = False
                if method == "message_send":
                    channel = wake.get("channel", "slack")
                    target = wake.get("target")
                    if not target:
                        append_dead_letter(event, "missing_message_target")
                        state["cursors"][name] = event_id
                        save_state(args.state, state)
                        continue
                    ok = send_message(channel, target, message, timeout)

                elif method == "agent_deliver":
                    # Run an agent turn and deliver its reply to a target.
                    session_key = wake.get("session_key") or OPENCLAW_SESSION_KEY
                    if not session_key:
                        append_dead_letter(event, "missing_session_key")
                        state["cursors"][name] = event_id
                        save_state(args.state, state)
                        continue
                    reply_channel = wake.get("reply_channel", "slack")
                    reply_to = wake.get("reply_to")  # e.g. Slack channel id
                    if not reply_to:
                        append_dead_letter(event, "missing_reply_to")
                        state["cursors"][name] = event_id
                        save_state(args.state, state)
                        continue
                    ok, _ = run_agent(
                        session_key,
                        message,
                        timeout,
                        deliver=True,
                        reply_channel=reply_channel,
                        reply_to=reply_to,
                    )

                elif method == "agent_gate":
                    # Run an agent turn, then decide whether to send a message.
                    session_key = wake.get("session_key") or OPENCLAW_SESSION_KEY
                    if not session_key:
                        append_dead_letter(event, "missing_session_key")
                        state["cursors"][name] = event_id
                        save_state(args.state, state)
                        continue
                    reply_channel = wake.get("reply_channel", "slack")
                    reply_to = wake.get("reply_to")
                    if not reply_to:
                        append_dead_letter(event, "missing_reply_to")
                        state["cursors"][name] = event_id
                        save_state(args.state, state)
                        continue
                    ok, reply, debug = run_agent(session_key, message, timeout, deliver=False)
                    reply = (reply or "").strip()
                    payload_message = (event.get("payload") or {}).get("message", "")
                    if log_path:
                        append_jsonl(
                            log_path,
                            {
                                "event_id": event.get("event_id"),
                                "timestamp": event.get("timestamp"),
                                "topic": event.get("topic"),
                                "stage": "agent_gate_reply",
                                "reply": reply,
                                "ok": ok,
                                "debug": debug,
                            },
                        )
                    if not ok:
                        pass
                    elif not reply or reply.upper() == "NO_REPLY":
                        if any(k in payload_message.lower() for k in ("disk exhausted", "disk out", "system crashed", "kernel panic", "host unreachable", "data corruption")):
                            fallback = f"CRITICAL: {payload_message}. Manual intervention required."
                            ok = send_message(reply_channel, reply_to, fallback, timeout)
                        else:
                            ok = True
                    else:
                        ok = send_message(reply_channel, reply_to, reply, timeout)

                else:
                    # sessions_send (agent wake, no delivery)
                    session_key = wake.get("session_key") or OPENCLAW_SESSION_KEY
                    if not session_key:
                        append_dead_letter(event, "missing_session_key")
                        state["cursors"][name] = event_id
                        save_state(args.state, state)
                        continue
                    ok, _ = run_agent(session_key, message, timeout, deliver=False)

                if ok:
                    state["cursors"][name] = event_id
                    state["attempts"].pop(event_id, None)
                    save_state(args.state, state)
                else:
                    # retry
                    attempts = state["attempts"].get(event_id, 0) + 1
                    state["attempts"][event_id] = attempts
                    retry = w.get("retry", {})
                    max_retry = int(retry.get("max", 3))
                    backoff = retry.get("backoff_seconds", [60, 300, 900])
                    if attempts >= max_retry:
                        append_dead_letter(event, "send_failed")
                        state["cursors"][name] = event_id
                        state["attempts"].pop(event_id, None)
                        save_state(args.state, state)
                    else:
                        # sleep backoff for this watcher
                        wait = backoff[min(attempts - 1, len(backoff) - 1)]
                        time.sleep(wait)

        if args.once:
            break
        if not any_work:
            time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()
