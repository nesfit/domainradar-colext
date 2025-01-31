import abc

from domrad_kafka_client.types import SimpleMessage


class ProcessorBase(abc.ABC):
    def __init__(self, config: dict):
        self._config = config

    @abc.abstractmethod
    def init(self) -> None:
        pass

    @abc.abstractmethod
    def process(self, key: bytes, value: bytes, partition: int, offset: int) -> list[SimpleMessage]:
        pass
