from typing import Type
import multiprocessing as mp

from . import util

from .client import KafkaClient
from .message_processor import *


def run_client(input_topic: str, processor_type: Type[AnyProcessor]):
    config = util.read_config()
    mp.set_start_method(config.get("client", {}).get("mp_start_method", "forkserver"))

    util.init_logging()
    kafka_client = KafkaClient(config, input_topic, processor_type)
    kafka_client.run()
    util.finalize_logging()


__all__ = [
    "run_client",
    "KafkaClient",
    "AnyProcessor",
    "ProcessorBase",
    "AsyncProcessorBase"
]
