"""util.py: Common utility functions for the collectors."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from typing import Optional

from pydantic import ValidationError

from common import log
from common import result_codes as rc
from common.models import IPProcessRequest, Result
from common.util import dump_model


def should_omit_ip(request: Optional[IPProcessRequest], collector_name: str) -> bool:
    return request is not None and request.collectors is not None and collector_name not in request.collectors


async def handle_top_level_exception(exc_info, component_id, key, result_class, topic):
    error_msg = str(exc_info)

    try:
        try:
            result = result_class(status_code=rc.INTERNAL_ERROR, error=error_msg)
        except ValidationError:
            result = Result(status_code=rc.INTERNAL_ERROR, error=error_msg)

        await topic.send(key=key, value=result)
    except Exception as e:
        log.get(component_id).k_unhandled_error(e, key, desc="Error sending the error message to the output topic",
                                                original_error=error_msg, topic=topic)


def make_top_level_exception_result(topic: str, exc_info, component_id, key, result_class) \
        -> tuple[str | None, bytes, bytes]:
    error_msg = str(exc_info)
    key_bytes = key if isinstance(key, bytes) else key.encode("utf-8")

    try:
        try:
            if issubclass(result_class, IPResult):
                result = result_class(status_code=rc.INTERNAL_ERROR, error=error_msg, collector=component_id)
            else:
                result = result_class(status_code=rc.INTERNAL_ERROR, error=error_msg)
        except ValidationError:
            result = Result(status_code=rc.INTERNAL_ERROR, error=error_msg)

        return topic, key_bytes, dump_model(result)
    except Exception as e:
        log.get(component_id).k_unhandled_error(e, key, desc="Error sending the error message to the output topic",
                                                original_error=error_msg, topic=topic)
        return None, b'', b''
