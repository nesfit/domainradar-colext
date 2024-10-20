"""__init__.py: The common module. Provides utility functions and a shared main function
for all the Python-based components."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from .util import (
    get_safe,
    timestamp_now_millis,
    ensure_model
)

__all__ = [
    "get_safe",
    "timestamp_now_millis",
    "ensure_model"
]
