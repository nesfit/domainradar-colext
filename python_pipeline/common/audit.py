import logging
from pprint import pprint

logger = logging.getLogger(__name__)


def log_warning(component_id: str, message: str, key: str | None = None, **kwargs):
    logger.warning(f"{component_id} warning: " + ("" if not key else f"[{key}] ") + message,
                   extra={"component": component_id, "key": key, **kwargs})


def log_unhandled_error(e: Exception, component_id: str, key: str | None = None, **kwargs):
    # TODO: Robust error handling...
    logger.error(f"{component_id} unexpected error" + "" if not key else f" [{key}]",
                 exc_info=e,
                 stack_info=True,
                 extra={"component": component_id, "key": key, **kwargs})
    logger.error(pprint(kwargs))
