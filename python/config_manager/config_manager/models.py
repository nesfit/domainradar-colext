"""models.py: The data models for the configuration change result messages."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from dataclasses import dataclass
from typing import Any

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class ConfigurationValidationError:
    propertyPath: str
    errorCode: int
    error: str | None = None
    soft: bool = False


@dataclass_json
@dataclass
class ConfigurationChangeResult:
    success: bool
    currentConfig: dict[str, Any]
    errors: list[ConfigurationValidationError] | None = None
    message: str | None = None
