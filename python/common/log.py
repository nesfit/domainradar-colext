"""log.py: A simple logging module that provides a custom logger with additional methods for key-based logging."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import datetime
import logging
import sys
from pprint import pformat
from typing import cast


class _CustomLoggerT(logging.Logger):
    """
    Custom logger class that extends the standard Python logging.Logger class.

    This class adds additional methods for trace logging and key-based logging. It also includes attributes for
    stderr and Kafka handlers, which can be used to direct log output to stderr or a Kafka topic, respectively.
    """

    k_stderr_handler: any  # Handler for directing log output to stderr
    k_kafka_handler: any  # Handler for directing log output to a Kafka topic

    def trace(self, message, *args, **kwargs):
        """
        Logs a trace level message.

        This method is similar to the standard logging methods (e.g., debug, info, etc.), but it logs at the trace
        level, which is lower than the debug level.

        Args:
            message (str): The message to be logged.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        pass

    def k_trace(self, message: str, key: str | None = None, *args, **kwargs):
        """
        Logs a trace level message with a key.

        This method is similar to the trace method, but it includes a key in the log output. The key can be used to
        group related log messages.

        Args:
            message (str): The message to be logged.
            key (str | None): The key to be included in the log output. If None, a default key is used.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        pass

    def k_debug(self, message: str, key: str | None = None, *args, **kwargs):
        """
        Logs a debug level message with a key.

        This method is similar to the standard debug method, but it includes a key in the log output. The key can be
        used to group related log messages.

        Args:
            message (str): The message to be logged.
            key (str | None): The key to be included in the log output. If None, a default key is used.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        pass

    def k_info(self, message: str, key: str | None = None, *args, **kwargs):
        """
        Logs an info level message with a key.

        This method is similar to the standard info method, but it includes a key in the log output. The key can be
        used to group related log messages.

        Args:
            message (str): The message to be logged.
            key (str | None): The key to be included in the log output. If None, a default key is used.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        pass

    def k_warning(self, message: str, key: str | None = None, *args, e: BaseException = None, **kwargs):
        """
        Logs a warning level message with a key.

        This method is similar to the standard warning method, but it includes a key in the log output. The key can be
        used to group related log messages.

        Args:
            message (str): The message to be logged.
            key (str | None): The key to be included in the log output. If None, a default key is used.
            e (BaseException, optional): The exception to be logged. If provided, the traceback is included in the log output.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        pass

    def k_unhandled_error(self, e: BaseException, key: str | None = None, **kwargs):
        """
        Logs an error level message for an unhandled exception with a key.

        This method is used to log unhandled exceptions. It includes a key in the log output and logs the traceback
        of the exception.

        Args:
            e (BaseException): The unhandled exception to be logged.
            key (str | None): The key to be included in the log output. If None, a default key is used.
            **kwargs: Arbitrary keyword arguments.
        """
        pass


NO_KEY_STR = "no key"


class ExtraFormatter(logging.Formatter):
    """A custom log message formatter that includes event keys and the 'properties' additional data."""

    def __init__(self, fmt=None, datefmt=None, style='%', validate=True, *, defaults=None):
        super().__init__(fmt="[{asctime}] [{levelname}] [{name}#{thread}-{threadName}] {{{event_key}}} {message}",
                         style='{', datefmt="")

    def formatTime(self, record, datefmt=None):
        ct = datetime.datetime.fromtimestamp(record.created).astimezone()
        return f"{ct:%Y-%m-%dT%H:%M:%S},{record.msecs:g}{ct:%z}"

    def formatMessage(self, record):
        if not hasattr(record, "event_key"):
            record.event_key = NO_KEY_STR
        formatted = super().formatMessage(record)
        if hasattr(record, "properties"):
            properties = record.properties
            if properties is not None and len(properties) > 0:
                formatted += "\nProperties: " + pformat(properties)
        return formatted


def init(component_id: str) -> _CustomLoggerT:
    # Define a constant for the trace logging level
    trace_const = logging.DEBUG - 5

    # Define the implementations of the custom logging methods to extend Python's logger with
    def k_debug(self, message: str, key: str | None = None, *args, **kwargs):
        self.debug(message, *args, extra={"event_key": key or NO_KEY_STR, "properties": kwargs})

    def k_info(self, message: str, key: str | None = None, *args, **kwargs):
        self.info(message, *args, extra={"event_key": key or NO_KEY_STR, "properties": kwargs})

    def k_warning(self, message: str, key: str | None = None, *args, e: BaseException = None, **kwargs):
        self.warning(message, *args, exc_info=e,
                     extra={"event_key": key or NO_KEY_STR, "properties": kwargs})

    def k_unhandled_error(self, e: BaseException, key: str | None = None, **kwargs):
        self.error("Unexpected error!", exc_info=e, stack_info=True,
                   extra={"event_key": key or NO_KEY_STR, "properties": kwargs})

    def trace(self, message, *args, **kwargs):
        if self.isEnabledFor(trace_const):
            self._log(trace_const, message, args, **kwargs)

    def k_trace(self, message: str, key: str | None = None, *args, **kwargs):
        self.trace(message, *args, extra={"event_key": key or "no key", "properties": kwargs})

    # Add the custom logging methods to the logger class if they don't already exist
    logger_class = logging.getLoggerClass()
    if not hasattr(logger_class, "k_unhandled_error"):
        setattr(logger_class, "trace", trace)
        setattr(logger_class, "k_trace", k_trace)
        setattr(logger_class, "k_debug", k_debug)
        setattr(logger_class, "k_info", k_info)
        setattr(logger_class, "k_warning", k_warning)
        setattr(logger_class, "k_unhandled_error", k_unhandled_error)

    # Get the logger for the component and set its level
    logger = logging.getLogger(component_id)

    # Return the configured logger
    return cast(_CustomLoggerT, logger)


def get(component_id: str) -> _CustomLoggerT:
    # noinspection PyTypeChecker
    logger = logging.getLogger(component_id)
    if not hasattr(logger, "k_unhandled_error"):
        # stub initialization
        init(component_id)
    return cast(_CustomLoggerT, logger)
