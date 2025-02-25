import abc
import dataclasses
import logging
from typing import Awaitable, TypeVar, Generic, Literal

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


class _KafkaMessageProcessorBase(Generic[TKey, TValue], abc.ABC):
    """Abstract base class for Kafka message processors.

    This class defines a common interface and shared functionality for both synchronous
    and asynchronous Kafka message processors. Derived classes must implement the abstract
    methods for deserialization, processing, and error handling.
    """
    ERROR_RATE_LIMITED = 1

    def __init__(self, config: dict):
        """
        Initialize the processor.

        :param config: Dictionary with the app configuration.
        """
        self._config = config
        self._logger = logging.getLogger("worker")

    def get_rl_bucket_key(self, message: Message[TKey, TValue]) -> str | Literal['default'] | None:
        """
        Get the rate limiter bucket key for the given message.

        :param message: The message to get the bucket key for.
        :return: The bucket key as a string, 'default' to use the default global rate limiter, or None to disable rate
                 limiting for this message.
        """
        return None

    @abc.abstractmethod
    def deserialize(self, message: Message[TKey, TValue]) -> None:
        """
        Deserialize the raw message data.
        This method should modify the input `message` object by settings its `key` and `value` fields
        based on `key_raw` and `value_raw`.

        :param message: The message to deserialize.
        """
        pass

    @abc.abstractmethod
    def process(self, message: Message[TKey, TValue]) -> \
            list[SimpleMessage] | Awaitable[list[SimpleMessage]]:
        """
        Process the given message and return a list of messages to respond with.
        This method returns a list if used within a `SyncKafkaMessageProcessor`, or an awaitable if used
        within an `AsyncKafkaMessageProcessor`.

        :param message: The message to process. It contains the key and value deserialized by `self.deserialize`.
        :return: A list of SimpleMessage tuples of (output topic, key bytes, value bytes), or an awaitable that
                 resolves to such a list.
        """
        pass

    @abc.abstractmethod
    def process_error(self, message: Message[TKey, TValue], error: BaseException | int) -> list[SimpleMessage]:
        """
        Handle an error that occurred during message processing and return a list of messages to respond with.
        This method is called when an unhandled exception occurs during the call to `self.deserialize` or
        `self.process`.
        It is also called when the internal rate limiter is triggered for the `message`. In this case, the `error`
        parameter is an `int` set to `self.ERROR_RATE_LIMITED`.

        :param message: The message that caused the error.
        :param error: The error that occurred, either as an exception or an error code.
        :return: A list of SimpleMessage objects of (output topic, key bytes, value bytes) to be sent
                 as error responses.
        """
        pass


class SyncKafkaMessageProcessor(_KafkaMessageProcessorBase[TKey, TValue]):
    @abc.abstractmethod
    def process(self, message: Message[TKey, TValue]) -> list[SimpleMessage]:
        pass


class AsyncKafkaMessageProcessor(_KafkaMessageProcessorBase[TKey, TValue]):
    @abc.abstractmethod
    async def process(self, message: Message[TKey, TValue]) -> list[SimpleMessage]:
        pass


KafkaMessageProcessor = SyncKafkaMessageProcessor | AsyncKafkaMessageProcessor
