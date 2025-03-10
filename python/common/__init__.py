"""__init__.py: The common module. Provides utility functions and a shared main function
for all the Python-based components."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"


from .util import (
    read_config,
    get_safe,
    timestamp_now_millis,
    ensure_model,
    dump_model
)

__all__ = [
    "read_config",
    "get_safe",
    "timestamp_now_millis",
    "ensure_model",
    "dump_model"
]
