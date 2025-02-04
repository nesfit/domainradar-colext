import logging
import logging.config
import logging.handlers
import os
import tomllib
import platform
import multiprocessing as mp
from typing import Optional

_config_file = None
_config: dict | None = None
_log_queue: Optional[mp.Queue] = None
_log_queue_listener: logging.handlers.QueueListener | None = None


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
    return _config_file


def read_config() -> dict:
    """Reads the configuration file specified by the environment variable APP_CONFIG_FILE."""
    global _config
    if _config:
        return _config

    with open(get_config_file(), "rb") as f:
        _config = tomllib.load(f)
        return _config


def init_logging() -> None:
    global _log_queue, _log_queue_listener, _config
    assert _config is not None

    trace_const = logging.DEBUG - 5
    if not hasattr(logging, "TRACE"):
        logging.addLevelName(trace_const, 'TRACE')
        setattr(logging, "TRACE", trace_const)

    class PassthroughHandler:
        def handle(self, record):
            if record.name == "root":
                logger = logging.getLogger()
            else:
                logger = logging.getLogger(record.name)

            if logger.isEnabledFor(record.levelno):
                logger.handle(record)

    config = _config.get("logging", {
        'version': 1,
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO'
            }
        },
        'root': {
            'handlers': ['console'],
            'level': 'INFO'
        }
    })

    _log_queue = mp.Queue()
    logging.config.dictConfig(config)
    _log_queue_listener = logging.handlers.QueueListener(_log_queue, PassthroughHandler())
    _log_queue_listener.start()


def get_worker_logger_config() -> dict:
    global _log_queue, _config
    assert _config is not None
    assert _log_queue is not None

    level = (_config.get("logging", {})
             .get("loggers", {})
             .get("worker", {})
             .get("level", "INFO"))

    return {
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'queue': {
                'class': 'logging.handlers.QueueHandler',
                'queue': _log_queue
            }
        },
        'root': {
            'handlers': ['queue'],
            'level': level
        }
    }


def finalize_logging():
    global _log_queue_listener
    if _log_queue_listener:
        _log_queue_listener.stop()
        _log_queue_listener = None


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
