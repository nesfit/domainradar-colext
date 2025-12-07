import os
from typing import Type, Callable
import multiprocessing as mp

from . import util

from .client import KafkaClient
from .message_processor import *
from .types import SimpleMessage


def run_client(input_topic: str, processor_type: Type[KafkaMessageProcessor], 
               app_id_override: str | Callable[[dict[str, Any]], str | None] | None = None):
    config = util.read_config()
    mp.set_start_method(config.get("client", {}).get("mp_start_method", "forkserver"))

    if callable(app_id_override):
        app_id_override = app_id_override(config)
    if isinstance(app_id_override, str):
        config["client"]["app_id"] = app_id_override

    if os.getenv("DOMRAD_PROCESS_STANDALONE"):
        from .standalone import run_cli
        run_cli(config, processor_type)
    else:
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
