import logging
from pprint import pprint

logger = logging.getLogger(__name__)


def log_unhandled_error(e: Exception, component_id: str, key: str | None = None, **kwargs):
    # TODO: Robust error handling...
    logger.error(f"Error processing {component_id} data" + "" if not key else f" key {key}",
                 exc_info=e,
                 stack_info=True,
                 extra={"component": component_id, "key": key, **kwargs})
    logger.error(pprint(kwargs))
