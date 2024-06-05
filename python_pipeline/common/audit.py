import logging
import sys
from pprint import pformat


class ExtraFormatter(logging.Formatter):
    def __init__(self):
        super().__init__(fmt="[%(asctime)s] [%(levelname)s] [%(component)s#%(threadName)s] {%(event_key)s} %(message)s",
                         style='%')

    def formatMessage(self, record):
        formatted = super().formatMessage(record)
        if hasattr(record, "properties"):
            properties = record.properties
            if properties is not None and len(properties) > 0:
                formatted += "\nProperties: " + pformat(properties)
        return formatted


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.INFO)
formatter = ExtraFormatter()
handler.setFormatter(formatter)
logger.addHandler(handler)


def log_info(component_id: str, message: str, key: str | None = None, **kwargs):
    logger.info(message,
                extra={"component": component_id, "event_key": key or "no key", "properties": kwargs})


def log_warning(component_id: str, message: str, key: str | None = None, **kwargs):
    logger.warning("Warning: " + message,
                   extra={"component": component_id, "event_key": key or "no key", "properties": kwargs})


def log_unhandled_error(e: Exception, component_id: str, key: str | None = None, **kwargs):
    logger.error("Unexpected error:", exc_info=e, stack_info=True,
                 extra={"component": component_id, "event_key": key or "no key", "properties": kwargs})
