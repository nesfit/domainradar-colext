from typing import Type
from . import util

from .client import KafkaClient
from .message_processor import ProcessorBase


def run_client(input_topic: str, processor_type: Type[ProcessorBase]):
    config = util.read_config()
    util.setup_logging(config, "client")
    kafka_client = KafkaClient(config, input_topic, processor_type)
    kafka_client.run()


__all__ = [
    "run_client",
    "KafkaClient",
    "ProcessorBase"
]
