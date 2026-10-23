import os
import tempfile

from sketchstream.config import DEFAULT_CONFIG_PATH, load_config


def test_load_default_config():
    cfg = load_config(DEFAULT_CONFIG_PATH)
    assert "stream" in cfg and "sketches" in cfg
    assert cfg["sketches"]["cms_width"] == 2000


def test_load_missing_config_returns_empty():
    assert load_config("does/not/exist.yaml") == {}


def test_load_custom_config_roundtrip():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "cfg.yaml")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("sketches:\n  cms_width: 512\n  hh_capacity: 64\n")
        cfg = load_config(path)
        assert cfg["sketches"]["cms_width"] == 512
        assert cfg["sketches"]["hh_capacity"] == 64
