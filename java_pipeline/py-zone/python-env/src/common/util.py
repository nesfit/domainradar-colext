"""util.py: A collection of shared utility functions for all the components."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import logging
import sys
from datetime import datetime
from typing import TypeVar, Type, Any

from pydantic import ValidationError

_logger = logging.getLogger("stub")
_logger.setLevel(logging.INFO)
_logger.addHandler(logging.StreamHandler(sys.stderr))
_config: dict | None = None
_config_file: str | None = None
_last_config_modify_time = -1


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
    """
    Validates the provided data against the specified Pydantic model class.

    This function takes a model class and a dictionary of data as input. It attempts to validate the data against
    the model class using the model's `model_validate` method. If the data is valid, the function returns the validated
    model. If the data is not valid, the function logs a warning and returns None.

    If an exception occurs during the validation process, the function logs an error and returns None.

    Args:
        model_class (Type[TModel]): The class of the model against which to validate the data.
        data (dict | None): The data to validate. If None, the function immediately returns None.

    Returns:
        TModel | None: The validated model if the data is valid, None otherwise.
    """
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
