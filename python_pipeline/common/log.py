import logging
import sys
from pprint import pformat


class _CustomLoggerT(logging.Logger):
    def trace(self, message, *args, **kwargs):
        pass

    def k_trace(self, message: str, key: str | None, *args, **kwargs):
        pass

    def k_debug(self, message: str, key: str | None, *args, **kwargs):
        pass

    def k_info(self, message: str, key: str | None, *args, **kwargs):
        pass

    def k_warning(self, message: str, key: str | None, *args, **kwargs):
        pass

    def k_unhandled_error(self, e: Exception, key: str | None, **kwargs):
        pass


NO_KEY_STR = "no key"


class ExtraFormatter(logging.Formatter):
    def __init__(self):
        super().__init__(fmt="[%(asctime)s] [%(levelname)s] [%(name)s#%(threadName)s] {%(event_key)s} %(message)s",
                         style='%')

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

    def k_warning(self, message: str, key: str | None = None, *args, **kwargs):
        self.warning(message, *args, extra={"event_key": key or NO_KEY_STR, "properties": kwargs})

    def k_unhandled_error(self, e: Exception, key: str | None = None, **kwargs):
        self.error("Unexpected error!", exc_info=e, stack_info=True,
                   extra={"event_key": key or NO_KEY_STR, "properties": kwargs})

    def trace(self, message, *args, **kwargs):
        if self.isEnabledFor(trace_const):
            self._log(trace_const, message, args, **kwargs)

    def k_trace(self, message: str, key: str | None = None, *args, **kwargs):
        self.trace(message, *args, extra={"event_key": key or "no key", "properties": kwargs})

    component_config = config.get(component_id, {})
    audit_log_level = component_config.get("audit_log_level")

    logging.addLevelName(trace_const, 'TRACE')
    setattr(logging, "TRACE", trace_const)

    logger_class = logging.getLoggerClass()
    setattr(logger_class, "trace", trace)
    setattr(logger_class, "k_trace", k_trace)
    setattr(logger_class, "k_debug", k_debug)
    setattr(logger_class, "k_info", k_info)
    setattr(logger_class, "k_warning", k_warning)
    setattr(logger_class, "k_unhandled_error", k_unhandled_error)

    logger = logging.getLogger(component_id)
    logger.setLevel(audit_log_level or logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(trace_const)
    formatter = ExtraFormatter()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    # noinspection PyTypeChecker
    return logger


def get(component_id: str) -> _CustomLoggerT:
    # noinspection PyTypeChecker
    return logging.getLogger(component_id)
