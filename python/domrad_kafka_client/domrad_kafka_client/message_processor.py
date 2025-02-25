import abc
import dataclasses
import logging
from typing import Awaitable, TypeVar, Generic

from domrad_kafka_client.types import SimpleMessage

TKey = TypeVar('TKey')
TValue = TypeVar('TValue')


@dataclasses.dataclass
class Message(Generic[TKey, TValue]):
    key_raw: bytes | None
    value_raw: bytes | None
    key: TKey | None
    value: TValue | None
    partition: int
    offset: int


class _ProcessorCommon(Generic[TKey, TValue], abc.ABC):
    ERROR_RATE_LIMITED = 1

    def __init__(self, config: dict):
        self._config = config
        self._logger = logging.getLogger("worker")

    def get_rl_bucket_key(self, message: Message[TKey, TValue]) -> str | None:
        return None

    @abc.abstractmethod
    def deserialize(self, message: Message[TKey, TValue]) -> None:
        pass

    @abc.abstractmethod
    def process(self, message: Message[TKey, TValue]) -> \
            list[SimpleMessage] | Awaitable[list[SimpleMessage]]:
        pass

    @abc.abstractmethod
    def process_error(self, message: Message[TKey, TValue], error: BaseException | int) -> list[SimpleMessage]:
        pass


class ProcessorBase(_ProcessorCommon[TKey, TValue]):
    @abc.abstractmethod
    def process(self, message: Message[TKey, TValue]) -> list[SimpleMessage]:
        pass


class AsyncProcessorBase(_ProcessorCommon[TKey, TValue]):
    @abc.abstractmethod
    async def process(self, message: Message[TKey, TValue]) -> list[SimpleMessage]:
        pass


AnyProcessor = ProcessorBase | AsyncProcessorBase
