import logging
import os
import ssl
import tomllib
from datetime import datetime

logger = logging.getLogger(__name__)


def ensure_data_dir() -> None:
    """Creates the directory specified by the environment variable APP_DATADIR if it does not exist."""
    data_dir = os.getenv("APP_DATADIR")
    if data_dir is not None and not os.path.exists(data_dir):
        logger.info("Creating data directory %s", data_dir)
        os.makedirs(data_dir)


def read_config() -> dict:
    """Reads the configuration file specified by the environment variable APP_CONFIG_FILE."""
    config_file = os.getenv("APP_CONFIG_FILE")
    if config_file is None:
        config_file = "./config.toml"

    if not os.path.isfile(config_file):
        raise FileNotFoundError("Configuration file not found: " + config_file)

    with open(config_file, "rb") as f:
        return tomllib.load(f)


def make_ssl_context(config) -> ssl.SSLContext | None:
    """Creates an SSL context with the specified configuration."""
    if "connection" not in config or "ssl" not in config["connection"] or not config["connection"].get("use_ssl"):
        logger.info("SSL not configured")
        return None

    logger.info("Loading SSL configuration")
    ssl_config = config["connection"]["ssl"]

    ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ssl_context.options |= ssl.OP_NO_SSLv2
    ssl_context.options |= ssl.OP_NO_SSLv3

    ssl_context.check_hostname = ssl_config.get("check_hostname", True)
    ssl_context.verify_mode = ssl.CERT_REQUIRED if ssl_config.get("server_verification_required",
                                                                  True) else ssl.CERT_NONE

    if "ca_file" in ssl_config:
        logger.debug("Loading CA certificate")
        ssl_context.load_verify_locations(cafile=ssl_config["ca_file"])
    else:
        logger.debug("Loading system CA certificates")
        if ssl_context.verify_mode == ssl.CERT_NONE:
            ssl_context.verify_mode = ssl.CERT_OPTIONAL
        ssl_context.load_default_certs()

    logger.info("Check hostname: %s, server verify mode: %s",
                ssl_context.check_hostname, ssl_context.verify_mode)

    logger.debug("Loading client certificate and key")
    ssl_context.load_cert_chain(certfile=ssl_config.get("client_cert_file"),
                                keyfile=ssl_config.get("client_key_file"),
                                password=ssl_config.get("client_key_password"))

    return ssl_context


def get_safe(data: dict, path: str) -> dict | None:
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
