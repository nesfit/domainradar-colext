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
