import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.filter_rules import match_filter


def test_simple_rule():
    event = {"payload": {"type": "rain"}}
    rule = {"field": "payload.type", "op": "==", "value": "rain"}
    assert match_filter(event, rule) is True


def test_any_all():
    event = {"payload": {"type": "rain", "city": "beijing"}}
    rule = {
        "all": [
            {"field": "payload.type", "op": "!=", "value": "sunny"},
            {"any": [
                {"field": "payload.city", "op": "==", "value": "beijing"},
                {"field": "payload.city", "op": "==", "value": "shanghai"},
            ]}
        ]
    }
    assert match_filter(event, rule) is True


def test_regex():
    event = {"payload": {"city": "beijing"}}
    rule = {"field": "payload.city", "op": "regex", "value": "bei.*"}
    assert match_filter(event, rule) is True
