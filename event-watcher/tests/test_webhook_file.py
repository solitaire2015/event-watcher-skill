import json
import os
import tempfile
from scripts.sources.webhook_file import read_events


def test_webhook_file_read():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "events.jsonl")
        with open(path, "w") as f:
            f.write(json.dumps({"event_id": "1", "payload": {"a": 1}}) + "\n")
            f.write(json.dumps({"event_id": "2", "payload": {"a": 2}}) + "\n")

        events, offset = read_events(path, 0, 10)
        assert len(events) == 2
        assert events[0]["event_id"] == "1"
        events2, offset2 = read_events(path, offset, 10)
        assert events2 == []
        assert offset2 == offset
