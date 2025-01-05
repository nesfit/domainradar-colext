"""__init__.py: The common module. Provides utility functions and a shared main function
for all the Python-based components."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

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
    """
    Main function to initialize and run the Python-based pipeline components.

    This function takes a module name as input, imports the module, and retrieves the component ID and application name
    from the module. It then gets the application object from the module and sets the logger for the utility module.

    The function ensures that the data directory exists and then calls the main method of the Faust application object.

    If an exception occurs during the execution of the application, the function logs the exception and re-raises it.

    Args:
        module (str): The name of the module to import.

    Raises:
        Exception: Any exception that occurs during the execution of the application.
    """
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
