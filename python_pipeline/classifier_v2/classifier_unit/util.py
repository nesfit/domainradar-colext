import logging
import os
import sys
import tomllib
import platform

_config_file = None
_config = None


def get_config_file() -> str:
    """
    Retrieves the path to the configuration file.

    The function retrieves the path from the environment variable APP_CONFIG_FILE.
    If the environment variable is not set, it defaults to './config.toml'. The function checks if the file
    exists and raises a FileNotFoundError if it does not.

    Returns:
        str: The path to the configuration file.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
    """
    global _config_file
    if _config_file:
        return _config_file

    config_file = os.getenv("APP_CONFIG_FILE")
    if config_file is None:
        config_file = "./config.toml"

    if not os.path.isfile(config_file):
        raise FileNotFoundError("Configuration file not found: " + config_file)

    _config_file = os.path.abspath(config_file)
    _last_config_modify_time = os.stat(_config_file).st_mtime
    return _config_file


def read_config() -> dict:
    """Reads the configuration file specified by the environment variable APP_CONFIG_FILE."""
    global _config
    if _config:
        return _config

    with open(get_config_file(), "rb") as f:
        _config = tomllib.load(f)
        return _config


def setup_logging(config: dict, section: str, worker: bool = False) -> None:
    config = config.get("logging", {}).get(section, {})

    logger = logging.getLogger()
    logger.setLevel(config.get("min_level", "INFO"))

    pid = os.getpid() if worker else "client"
    formatter = logging.Formatter(f'[%(name)s][{pid}][%(levelname)s]\t%(asctime)s: %(message)s')

    if "stdout_level" in config and config["stdout_level"] != "disabled":
        out_h = logging.StreamHandler(stream=sys.stdout)
        out_h.setLevel(config.get("stdout_level", "INFO"))
        out_h.setFormatter(formatter)
        logger.addHandler(out_h)

    if "stderr_level" in config and config["stderr_level"] != "disabled":
        err_h = logging.StreamHandler(stream=sys.stderr)
        err_h.setLevel(config.get("stderr_level", "WARN"))
        err_h.setFormatter(formatter)
        logger.addHandler(err_h)


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
    ret["group.id"] = config["client"]["app_id"]
    ret = ret | config.get("consumer", {})

    # Exactly-once semantics
    ret["enable.auto.commit"] = False
    ret["enable.auto.offset.store"] = False

    return ret


def make_producer_settings(config: dict) -> dict:
    ret = make_client_settings(config)
    ret["transactional.id"] = f'{config["client"]["app_id"]}-at-{platform.node()}'
    ret = ret | config.get("producer", {})

    return ret
