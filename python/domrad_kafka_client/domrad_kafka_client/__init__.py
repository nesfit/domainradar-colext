from typing import Type
import multiprocessing as mp

from . import util

from .client import KafkaClient
from .message_processor import *
from .types import SimpleMessage


def run_client(input_topic: str, processor_type: Type[KafkaMessageProcessor], app_id_override: str | None = None):
    config = util.read_config()
    mp.set_start_method(config.get("client", {}).get("mp_start_method", "forkserver"))
    if app_id_override:
        config["client"]["app_id"] = app_id_override

    util.init_logging()
    kafka_client = KafkaClient(config, input_topic, processor_type)
    kafka_client.run()
    util.finalize_logging()


__all__ = [
    "run_client",
    "KafkaClient",
    "KafkaMessageProcessor",
    "SyncKafkaMessageProcessor",
    "AsyncKafkaMessageProcessor",
    "SimpleMessage",
    "Message"
]
