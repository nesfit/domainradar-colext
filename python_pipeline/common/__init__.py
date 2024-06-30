from .custom_codecs import StringCodec, PydanticCodec

from .util import (
    ensure_data_dir,
    read_config,
    make_ssl_context,
    make_app,
    get_safe,
    timestamp_now_millis,
    ensure_model
)

__all__ = [
    "main",
    "ensure_data_dir",
    "read_config",
    "make_ssl_context",
    "make_app",
    "get_safe",
    "timestamp_now_millis",
    "ensure_model",
    "StringCodec",
    "PydanticCodec"
]


def main(component_id: str, app):
    from common import ensure_data_dir, log, read_config, util
    util._logger = log.init(component_id, read_config())

    try:
        ensure_data_dir()
        app.main()
    except Exception as e:
        log.get(component_id).k_unhandled_error(e, None)
        raise e
