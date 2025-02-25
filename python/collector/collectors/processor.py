import abc
from typing import TypeVar, Type

from pydantic import BaseModel

from collectors.util import make_top_level_exception_result
from common import ensure_model
from common.util import dump_model
from domrad_kafka_client import AsyncKafkaMessageProcessor, Message, SimpleMessage
import common.result_codes as rc

TCollectorKey = TypeVar('TCollectorKey')
TCollectorRequest = TypeVar('TCollectorRequest', bound=BaseModel)


class BaseAsyncCollectorProcessor(AsyncKafkaMessageProcessor[TCollectorKey, TCollectorRequest], abc.ABC):

    def __init__(self, config: dict, component_name: str, output_topic: str, key_class: Type[TCollectorKey],
                 value_class: Type[TCollectorRequest]):
        super().__init__(config)
        self._component_name = component_name
        self._output_topic = output_topic
        self._key_class = key_class
        self._value_class = value_class

    def deserialize(self, message: Message[TCollectorKey, TCollectorRequest]):
        if issubclass(self._key_class, str):
            message.key = message.key_raw.decode("utf-8")
        else:
            message.key = ensure_model(self._key_class, message.key_raw)
        message.value = ensure_model(self._value_class, message.value_raw)

    def process_error(self, message: Message[TCollectorKey, TCollectorRequest], error: BaseException | int) \
            -> list[SimpleMessage]:
        if isinstance(error, BaseException):
            self._logger.k_unhandled_error(error, message.key)
            res = make_top_level_exception_result(self._output_topic, error, self._component_name, message.key_raw,
                                                  self._value_class)
        elif error == self.ERROR_RATE_LIMITED:
            self._logger.k_info("Rate limited", message.key)
            return [
                (self._output_topic, message.key_raw, dump_model(self._value_class(status_code=rc.LOCAL_RATE_LIMIT)))
            ]
        else:
            self._logger.k_unhandled_error(ValueError(), message.key, error_id=error)
            res = make_top_level_exception_result(self._output_topic, f"Error {error}",
                                                  self._component_name, message.key_raw, self._value_class)

        if res[0] is None: return []
        return [res]
