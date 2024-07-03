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


def main(module: str):
    from common import ensure_data_dir, log, util
    import importlib

    app_module = importlib.import_module(module)
    component_id = app_module.COMPONENT_NAME
    app_name: str = app_module.COLLECTOR if hasattr(app_module, "COLLECTOR") else app_module.COMPONENT_NAME
    app_name = app_name.replace('-', '_') + "_app"
    app_obj = getattr(app_module, app_name)

    util._logger = log.get(component_id)

    try:
        ensure_data_dir()
        app_obj.main()
    except Exception as e:
        log.get(component_id).k_unhandled_error(e, None)
        raise e
