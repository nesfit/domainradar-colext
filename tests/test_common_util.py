import json
import time

import pytest

from common import util
from common.models import IPToProcess


@pytest.fixture(autouse=True)
def reset_common_util_globals():
    util._config = None
    util._config_file = None
    util._last_config_modify_time = -1
    yield
    util._config = None
    util._config_file = None
    util._last_config_modify_time = -1


def test_get_config_file_uses_env(tmp_path, monkeypatch):
    config_path = tmp_path / "config.toml"
    config_path.write_text("[test]\nvalue = 1\n")
    monkeypatch.setenv("APP_CONFIG_FILE", str(config_path))

    resolved = util.get_config_file()

    assert resolved == str(config_path.resolve())


def test_get_config_file_missing_raises(monkeypatch, tmp_path):
    missing_path = tmp_path / "missing.toml"
    monkeypatch.setenv("APP_CONFIG_FILE", str(missing_path))

    with pytest.raises(FileNotFoundError):
        util.get_config_file()


def test_read_config_reads_toml(tmp_path, monkeypatch):
    config_path = tmp_path / "config.toml"
    config_path.write_text("[section]\nvalue = 42\n")
    monkeypatch.setenv("APP_CONFIG_FILE", str(config_path))

    data = util.read_config()

    assert data["section"]["value"] == 42


def test_read_config_returns_cached(monkeypatch, tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text("[section]\nvalue = 1\n")
    monkeypatch.setenv("APP_CONFIG_FILE", str(config_path))
    util._config = {"cached": True}

    data = util.read_config()

    assert data == {"cached": True}


def test_get_safe_handles_missing_paths():
    data = {"a": {"b": {"c": 1}}}

    assert util.get_safe(data, "a.b.c") == 1
    assert util.get_safe(data, "a.b.d") is None
    assert util.get_safe(None, "a") is None


def test_timestamp_now_millis_is_int_and_recent():
    before = int(time.time() * 1e3)
    stamp = util.timestamp_now_millis()
    after = int(time.time() * 1e3)

    assert isinstance(stamp, int)
    assert before <= stamp <= after


def test_ensure_model_parses_bytes_and_aliases():
    payload = json.dumps({"dn": "example.com", "ip": "1.2.3.4"}).encode("utf-8")

    model = util.ensure_model(IPToProcess, payload)

    assert isinstance(model, IPToProcess)
    assert model.domain_name == "example.com"
    assert model.ip == "1.2.3.4"


def test_ensure_model_validation_error_returns_none():
    payload = {"dn": "example.com"}

    model = util.ensure_model(IPToProcess, payload)

    assert model is None


def test_dump_model_serializes_with_aliases():
    model = IPToProcess(dn="example.com", ip="1.2.3.4")

    raw = util.dump_model(model)

    assert isinstance(raw, (bytes, bytearray))
    decoded = json.loads(raw.decode("utf-8"))
    assert decoded == {"dn": "example.com", "ip": "1.2.3.4"}


def test_dump_model_serializes_dict():
    raw = util.dump_model({"a": 1})

    assert json.loads(raw.decode("utf-8")) == {"a": 1}
