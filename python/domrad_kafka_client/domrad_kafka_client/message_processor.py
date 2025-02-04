import abc
import logging

from domrad_kafka_client.types import SimpleMessage


class ProcessorBase(abc.ABC):
    def __init__(self, config: dict):
        self._config = config
        self._logger = logging.getLogger("worker")

    @abc.abstractmethod
    def process(self, key: bytes, value: bytes, partition: int, offset: int) -> list[SimpleMessage]:
        pass


class AsyncProcessorBase(abc.ABC):
    def __init__(self, config: dict):
        self._config = config
        self._logger = logging.getLogger("worker")

    @abc.abstractmethod
    async def process(self, key: bytes, value: bytes, partition: int, offset: int) -> list[SimpleMessage]:
        pass


AnyProcessor = ProcessorBase | AsyncProcessorBase
