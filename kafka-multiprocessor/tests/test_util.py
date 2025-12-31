import logging

from kafka_multiprocessor import util


def _reset_util_state():
    util._config_file = None
    util._config = None
    util._log_queue = None
    util._log_queue_listener = None


def test_get_config_file_env_override(tmp_path, monkeypatch):
    config_path = tmp_path / "config.toml"
    config_path.write_text("[client]\napp_id = \"test\"\n", encoding="utf-8")
    monkeypatch.setenv("APP_CONFIG_FILE", str(config_path))
    _reset_util_state()

    assert util.get_config_file() == str(config_path.resolve())
    config = util.read_config()
    assert config["client"]["app_id"] == "test"


def test_make_consumer_settings_with_ssl():
    config = {
        "connection": {
            "brokers": ["kafka-1:9092", "kafka-2:9092"],
            "use_ssl": True,
            "ssl": {
                "ca_file": "/tmp/ca.crt",
                "client_cert_file": "/tmp/client.crt",
                "client_key_file": "/tmp/client.key",
                "client_key_password": "secret",
                "check_hostname": True,
            },
        },
        "client": {"app_id": "app-1"},
        "consumer": {"auto.offset.reset": "earliest"},
    }

    settings = util.make_consumer_settings(config)
    assert settings["bootstrap.servers"] == "kafka-1:9092,kafka-2:9092"
    assert settings["group.id"] == "app-1"
    assert settings["enable.auto.commit"] is False
    assert settings["enable.auto.offset.store"] is False
    assert settings["security.protocol"] == "SSL"
    assert settings["ssl.endpoint.identification.algorithm"] == "https"
    assert settings["auto.offset.reset"] == "earliest"


def test_add_logging_trace_level():
    util.add_logging_trace_level()
    assert hasattr(logging, "TRACE")
    logger = logging.getLogger("test")
    assert hasattr(logger, "trace")
