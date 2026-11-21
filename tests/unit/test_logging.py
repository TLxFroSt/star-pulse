"""Tests des formateurs de logs."""

import json
import logging

from star_pulse.common.logging import JsonFormatter, TextFormatter


def _record() -> logging.LogRecord:
    record = logging.makeLogRecord({"name": "t", "levelname": "INFO", "msg": "Bonjour"})
    record.content_sha256 = "abc"
    return record


def test_json_formatter_includes_extra_fields() -> None:
    payload = json.loads(JsonFormatter().format(_record()))
    assert payload["msg"] == "Bonjour"
    assert payload["content_sha256"] == "abc"


def test_text_formatter_appends_extra_fields() -> None:
    assert TextFormatter().format(_record()).endswith("Bonjour content_sha256=abc")
