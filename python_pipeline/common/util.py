import logging
import os
import ssl
import sys
import tomllib
from datetime import datetime
from typing import TypeVar, Type, Any, cast

import faust
from faust.serializers import codecs
from pydantic import ValidationError

from .custom_codecs import StringCodec, PydanticCodec

_logger = logging.getLogger("stub")
_logger.setLevel(logging.INFO)
_logger.addHandler(logging.StreamHandler(sys.stderr))
_config: dict | None = None
_config_file: str | None = None
_last_config_modify_time = -1


def ensure_data_dir() -> None:
    """Creates the directory specified by the environment variable APP_DATADIR if it does not exist."""
    data_dir = os.getenv("APP_DATADIR")
    if data_dir is not None and not os.path.exists(data_dir):
        _logger.info("Creating data directory %s", data_dir)
        os.makedirs(data_dir)


def get_config_file() -> str:
    global _config_file, _last_config_modify_time
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


def make_ssl_context(config) -> ssl.SSLContext | None:
    """Creates an SSL context with the specified configuration."""
    if "connection" not in config or "ssl" not in config["connection"] or not config["connection"].get("use_ssl"):
        _logger.info("SSL not configured")
        return None

    _logger.info("Loading SSL configuration")
    ssl_config = config["connection"]["ssl"]

    ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ssl_context.options |= ssl.OP_NO_SSLv2
    ssl_context.options |= ssl.OP_NO_SSLv3

    ssl_context.check_hostname = ssl_config.get("check_hostname", True)
    ssl_context.verify_mode = ssl.CERT_REQUIRED if ssl_config.get("server_verification_required",
                                                                  True) else ssl.CERT_NONE

    if "ca_file" in ssl_config:
        _logger.debug("Loading CA certificate")
        ssl_context.load_verify_locations(cafile=ssl_config["ca_file"])
    else:
        _logger.debug("Loading system CA certificates")
        if ssl_context.verify_mode == ssl.CERT_NONE:
            ssl_context.verify_mode = ssl.CERT_OPTIONAL
        ssl_context.load_default_certs()

    _logger.info("Check hostname: %s, server verify mode: %s",
                 ssl_context.check_hostname, ssl_context.verify_mode)

    _logger.debug("Loading client certificate and key")
    ssl_context.load_cert_chain(certfile=ssl_config.get("client_cert_file"),
                                keyfile=ssl_config.get("client_key_file"),
                                password=ssl_config.get("client_key_password"))

    return ssl_context


def make_app(name: str, config: dict) -> faust.App:
    from . import log

    """Creates a Faust application with the specified configuration."""
    # [connection] section
    connection_config = config.get("connection", {})
    # [component_name] section
    component_config = config.get(name, {})
    # [component_name.faust] section
    component_faust_config = component_config.get("faust", {})

    # Returns None if SSL is disabled / not configured
    ssl_context = make_ssl_context(config)

    codecs.register("str", StringCodec())
    codecs.register("pydantic", PydanticCodec())

    app = faust.App(component_config.get("app_id", "domrad-" + name),
                    broker=connection_config.get("brokers", "kafka://localhost:9092"),
                    broker_credentials=ssl_context,
                    debug=component_config.get("debug", False),
                    key_serializer="pydantic",
                    value_serializer="pydantic",
                    web_enabled=False,
                    **component_faust_config)

    log.inject_handler(log.get(name), app.logger, component_config)
    return app


# noinspection PyTypeChecker
def check_config_changes(component_id: str, app):
    global _last_config_modify_time, _config
    config = cast(dict, _config)
    if config.get(component_id, {}).get("watch_config", False):
        return

    from . import log

    stamp = os.stat(_config_file).st_mtime
    if stamp != _last_config_modify_time:
        _logger.info("Configuration file changed, reloading")
        _last_config_modify_time = stamp
        _config = None
        config = read_config()
        log.init(component_id, config)
        log.inject_handler(log.get(component_id), app.logger, config.get(component_id, {}))


def get_safe(data: dict, path: str) -> Any | None:
    """Gets a value from a nested dictionary, returning None if the path doesn't exist."""
    if data is None:
        return None

    try:
        for key in path.split("."):
            if data is None:
                return None
            data = data[key]
        return data
    except (KeyError, TypeError):
        return None


def timestamp_now_millis() -> int:
    """Returns the current UNIX timestamp in milliseconds."""
    return int(datetime.now().timestamp() * 1e3)


TModel = TypeVar('TModel')


def ensure_model(model_class: Type[TModel], data: dict | None) -> TModel | None:
    if data is None:
        return None

    try:
        return model_class.model_validate(data)
    except ValidationError as e:
        _logger.warning("Invalid model",
                        extra={"properties": {"input_data": data, "model": model_class.__name__, "errors": e.errors()}})
        return None
    except Exception as e:
        _logger.error("Error validating model", exc_info=e,
                      extra={"properties": {"input_data": data, "model": model_class.__name__}})
        return None
