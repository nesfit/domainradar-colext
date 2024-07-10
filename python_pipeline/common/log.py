import datetime
import logging
import sys
from pprint import pformat
from typing import cast


class _CustomLoggerT(logging.Logger):
    k_stderr_handler: any
    k_kafka_handler: any

    def trace(self, message, *args, **kwargs):
        pass

    def k_trace(self, message: str, key: str | None, *args, **kwargs):
        pass

    def k_debug(self, message: str, key: str | None, *args, **kwargs):
        pass

    def k_info(self, message: str, key: str | None, *args, **kwargs):
        pass

    def k_warning(self, message: str, key: str | None, *args, e: Exception = None, **kwargs):
        pass

    def k_unhandled_error(self, e: Exception, key: str | None, **kwargs):
        pass


NO_KEY_STR = "no key"


class ExtraFormatter(logging.Formatter):
    def __init__(self):
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


def init(component_id: str, config: dict) -> _CustomLoggerT:
    trace_const = logging.DEBUG - 5

    def k_debug(self, message: str, key: str | None = None, *args, **kwargs):
        self.debug(message, *args, extra={"event_key": key or NO_KEY_STR, "properties": kwargs})

    def k_info(self, message: str, key: str | None = None, *args, **kwargs):
        self.info(message, *args, extra={"event_key": key or NO_KEY_STR, "properties": kwargs})

    def k_warning(self, message: str, key: str | None = None, *args, e: Exception = None, **kwargs):
        self.warning(message, *args, exc_info=e,
                     extra={"event_key": key or NO_KEY_STR, "properties": kwargs})

    def k_unhandled_error(self, e: Exception, key: str | None = None, **kwargs):
        self.error("Unexpected error!", exc_info=e, stack_info=True,
                   extra={"event_key": key or NO_KEY_STR, "properties": kwargs})

    def trace(self, message, *args, **kwargs):
        if self.isEnabledFor(trace_const):
            self._log(trace_const, message, args, **kwargs)

    def k_trace(self, message: str, key: str | None = None, *args, **kwargs):
        self.trace(message, *args, extra={"event_key": key or "no key", "properties": kwargs})

    component_config = config.get(component_id, {})

    main_log_level = component_config.get("log_level", "INFO")
    log_level_stderr = component_config.get("log_level_stderr", "INFO")
    # log_level_kafka = component_config.get("log_level_kafka", "INFO")

    if not hasattr(logging, "TRACE"):
        logging.addLevelName(trace_const, 'TRACE')
        setattr(logging, "TRACE", trace_const)

    logger_class = logging.getLoggerClass()
    if not hasattr(logger_class, "k_unhandled_error"):
        setattr(logger_class, "trace", trace)
        setattr(logger_class, "k_trace", k_trace)
        setattr(logger_class, "k_debug", k_debug)
        setattr(logger_class, "k_info", k_info)
        setattr(logger_class, "k_warning", k_warning)
        setattr(logger_class, "k_unhandled_error", k_unhandled_error)

    logger = logging.getLogger(component_id)
    logger.setLevel(main_log_level)

    if hasattr(logger, "k_stderr_handler"):
        logger.k_stderr_handler.setLevel(log_level_stderr)
    else:
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(log_level_stderr)
        formatter = ExtraFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        setattr(logger, "k_stderr_handler", handler)

    # TODO: Kafka handler

    # noinspection PyTypeChecker
    return logger


def inject_handler(configured_logger: _CustomLoggerT, faust_logger: logging.Logger,
                   component_config: dict):
    should_be_injected = component_config.get("include_faust_logs_in_kafka", False)
    add = should_be_injected and not hasattr(faust_logger, "k_kafka_handler")
    remove = not should_be_injected and hasattr(faust_logger, "k_kafka_handler")

    if add:
        if not hasattr(configured_logger, "k_kafka_handler"):
            return
        handler = configured_logger.k_kafka_handler
        faust_logger.addHandler(handler)
        setattr(faust_logger, "k_kafka_handler", handler)
    elif remove:
        # noinspection PyUnresolvedReferences
        handler = faust_logger.k_kafka_handler
        faust_logger.removeHandler(handler)
        delattr(faust_logger, "k_kafka_handler")


def get(component_id: str) -> _CustomLoggerT:
    # noinspection PyTypeChecker
    logger = logging.getLogger(component_id)
    if not hasattr(logger, "k_unhandled_error"):
        # stub initialization
        init(component_id, {})
    return cast(_CustomLoggerT, logger)
