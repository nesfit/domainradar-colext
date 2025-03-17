import abc
from typing import TypeVar, Type

from pydantic import BaseModel, ValidationError

from common import ensure_model, dump_model
from common.models import IPResult, IPProcessRequest, Result, IPToProcess
from domrad_kafka_client import AsyncKafkaMessageProcessor, Message, SimpleMessage
import common.result_codes as rc

TCollectorKey = TypeVar('TCollectorKey')
TCollectorRequest = TypeVar('TCollectorRequest', bound=BaseModel)
TCollectorResponse = TypeVar('TCollectorResponse', bound=BaseModel)


class BaseAsyncCollectorProcessor(AsyncKafkaMessageProcessor[TCollectorKey, TCollectorRequest], abc.ABC):

    def __init__(self, config: dict, component_name: str, output_topic: str, key_class: Type[TCollectorKey],
                 value_class: Type[TCollectorRequest], result_class: Type[TCollectorResponse]):
        super().__init__(config)
        self._component_name = component_name

        if component_name.startswith("collector-"):
            self._collector_name = component_name[10:]
        else:
            self._collector_name = component_name

        self._output_topic = output_topic
        self._key_class = key_class
        self._value_class = value_class
        self._result_class = result_class

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
            res = self._make_result(error, message.key_raw)
        elif error == self.ERROR_RATE_LIMITED_IMMEDIATE or error == self.ERROR_RATE_LIMITED_WITH_TIMEOUT:
            self._logger.k_info("Rate limited", message.key)
            status_code = rc.LOCAL_RATE_LIMIT if error == self.ERROR_RATE_LIMITED_IMMEDIATE else rc.LRL_TIMEOUT
            res = self._make_result(None, message.key_raw, status_code)
        else:
            self._logger.k_unhandled_error(ValueError(), message.key, error_id=error)
            res = self._make_result(f"Unknown error ID: {error}", message.key_raw)

        if res[0] is None: return []
        return [res]

    def _make_result(self, error: BaseException | str | None, key_raw: bytes, code: int | None = None) \
        -> tuple[str | None, bytes, bytes]:

        error_msg = str(error) if error is not None else None
        code = code if code is not None else rc.INTERNAL_ERROR
        is_ip_result = issubclass(self._result_class, IPResult)

        try:
            if is_ip_result:
                result = self._result_class(status_code=code, error=error_msg, collector=self._collector_name)
            else:
                result = self._result_class(status_code=code, error=error_msg)
        except ValidationError:
            if is_ip_result:
                result = IPResult(status_code=code, error=error_msg, collector=self._collector_name)
            else:
                result = Result(status_code=code, error=error_msg)

        try:
            return self._output_topic, key_raw, dump_model(result)
        except Exception as e:
            self._logger.error("Cannot serialize top-level error response", exc_info=e)
            return None, b'', b''

    def _should_omit_ip(self, dn_ip: IPToProcess, request: IPProcessRequest | None) -> bool:
        result = request is not None and request.collectors is not None and self._collector_name not in request.collectors
        if result:
            self._logger.k_trace("Omitting IP %s", dn_ip.domain_name, dn_ip.ip)
        return result
