"""codes.py: Error codes for the configuration change request."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

OTHER = 1
"""Unspecified error."""
INVALID_MESSAGE = 2
"""Invalid input message format (configuration model deserialization error)."""
INVALID_PROPERTY = 3
"""No such configuration property exists."""
INVALID_TYPE = 4
"""Invalid data type of the provided value."""
OUT_OF_RANGE = 5
"""The provided value is out of the allowed range."""
READ_ONLY = 6
"""The property cannot be changed dynamically."""
MISSING = 7
"""The property must be explicitly defined."""
