"""Load experiment/stream parameters from a YAML file.

The same `configs/default.yaml` drives both the experiment harness and the live
demo: command-line flags always win, but when a flag is omitted its default is
taken from the config file. A missing file (or missing PyYAML) is not an error -
callers simply fall back to their built-in defaults.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

DEFAULT_CONFIG_PATH = "configs/default.yaml"


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """Return the parsed YAML config, or ``{}`` if it cannot be read."""
    path = path or DEFAULT_CONFIG_PATH
    if not os.path.exists(path):
        return {}
    try:
        import yaml
    except Exception:  # pragma: no cover - pyyaml is a declared dependency
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data if isinstance(data, dict) else {}


__all__ = ["load_config", "DEFAULT_CONFIG_PATH"]
