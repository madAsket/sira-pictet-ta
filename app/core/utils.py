from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_from_project_root(path: Path) -> Path:
    return path if path.is_absolute() else project_root() / path


def read_text_file(path: Path, *, missing_message: str) -> str:
    target = resolve_from_project_root(path)
    if not target.exists():
        raise FileNotFoundError(missing_message.format(path=target))
    return target.read_text(encoding="utf-8")


def extract_first_json_object(raw_text: str) -> dict[str, Any] | None:
    start = raw_text.find("{")
    if start == -1:
        return None

    depth = 0
    for index in range(start, len(raw_text)):
        char = raw_text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                snippet = raw_text[start : index + 1]
                try:
                    parsed = json.loads(snippet)
                except json.JSONDecodeError:
                    return None
                if isinstance(parsed, dict):
                    return parsed
                return None
    return None


def collapse_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()
