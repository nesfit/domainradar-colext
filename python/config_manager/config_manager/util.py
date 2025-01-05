"""util.py: Contains utility functions for reading the configuration file and creating Kafka client settings."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import os
import tomllib


def read_config() -> dict:
    """Reads the configuration file specified by the environment variable APP_CONFIG_FILE."""
    config_file = os.getenv("APP_CONFIG_FILE")
    if config_file is None:
        config_file = "./config.toml"

    if not os.path.isfile(config_file):
        raise FileNotFoundError("Configuration file not found: " + config_file)

    with open(config_file, "rb") as f:
        return tomllib.load(f)


def make_client_settings(config: dict) -> dict:
    ret = {
        "bootstrap.servers": ",".join(config["connection"]["brokers"]),
    }

    if config["connection"]["use_ssl"]:
        ret["security.protocol"] = "SSL"
        ret["ssl.ca.location"] = config["connection"]["ssl"]["ca_file"]
        ret["ssl.certificate.location"] = config["connection"]["ssl"]["client_cert_file"]
        ret["ssl.key.location"] = config["connection"]["ssl"]["client_key_file"]
        ret["ssl.key.password"] = config["connection"]["ssl"]["client_key_password"]
        ret["ssl.endpoint.identification.algorithm"] = \
            "https" if config["connection"]["ssl"]["check_hostname"] else "none"

    return ret


def make_consumer_settings(config: dict) -> dict:
    ret = make_client_settings(config)
    ret["group.id"] = config["manager"]["app_id"]
    ret = ret | config.get("consumer", {})

    return ret


def make_producer_settings(config: dict) -> dict:
    ret = make_client_settings(config)
    ret = ret | config.get("producer", {})

    return ret
