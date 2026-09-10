"""Bounded, single-line text for diagnostic identities and failure reasons."""

import json
from unicodedata import category


def diagnostic_text(value: object) -> str:
    try:
        return str(value)
    except Exception:
        # Diagnostics must not replace the failure, nor swallow cancellation.
        return "<diagnostic text unavailable>"


def quote_log_field(value: str) -> str:
    if len(value) > 4096:
        value = value[:4096] + "...[truncated]"
    encoded = json.dumps(value, ensure_ascii=False)
    return "".join(
        json.dumps(character, ensure_ascii=True)[1:-1]
        if category(character) in {"Cc", "Cf", "Zl", "Zp", "Cs"}
        else character
        for character in encoded
    )
