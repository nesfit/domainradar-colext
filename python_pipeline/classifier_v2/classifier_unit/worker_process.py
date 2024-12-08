import json
import logging
import multiprocessing as mp
import os
import queue
import signal
from datetime import datetime

import confluent_kafka as ck
import pandas as pd
import pyarrow as pa

from . import util
from .classifier_impl import make_classifier_impl

process = None


class WorkerProcess:
    def __init__(self, config: dict, to_process: mp.Queue,
                 processed: mp.Queue, topic_out: str, worker_id: int):
        self._config = config
        self._logger = logging.getLogger(f"worker-{worker_id}")
        self._to_process = to_process
        self._processed = processed
        self._topic = topic_out
        self._running = True

        producer_settings = util.make_producer_settings(self._config)
        producer_settings["on_delivery"] = self._delivery_callback

        self._logger.info("Initializing producer")
        # noinspection PyArgumentList
        self._producer = ck.Producer(producer_settings, logger=self._logger)

        self._logger.info("Initializing pipeline")
        self._pipeline = make_classifier_impl(config)
        self._pipeline.init()

    def run(self):
        while self._running:
            try:
                partition, offset, value = self._to_process.get(True, 5.0)
                self._logger.debug("Processing at partition = %s, offset = %s", partition, offset)
                self._process_message(partition, offset, value)
                self._producer.poll(0)  # Trigger delivery
                self._processed.put((partition, offset), True, None)
            except queue.Empty:
                pass
            except KeyboardInterrupt:
                self._logger.info("Interrupted. Shutting down")
                self._running = False
            except Exception as e:
                self._logger.error("Unexpected error. Shutting down", exc_info=e)
                self._running = False

        self._logger.info("Finished (PID %s)", os.getpid())
        self._producer.flush(5)  # TODO: Configurable?

    def close(self):
        self._running = False

    def _delivery_callback(self, err, msg):
        if err:
            self._logger.warning("Result failed delivery: %s", err)
            key = msg.key()
            value = msg.value()
            if key is not None and value is not None:
                # Retry
                self._producer.produce(self._topic, key=key, value=value)

    def _process_message(self, partition: int, offset: int, value: bytes):
        try:
            df = pd.read_feather(pa.BufferReader(value))
        except Exception as e:
            self._logger.error("Cannot read value at partition = %s, offset = %s",
                               partition, offset, exc_info=e)
            return

        try:
            results = self._pipeline.classify(df)
        except Exception as e:
            self._process_erroneous_df(partition, offset, df, e)
            return

        result: dict
        for result in results:
            if "domain_name" not in result:
                self._logger.warning("Missing domain_name in a classification result at partition = %s, offset = %s",
                                     partition, offset)
                continue

            self._producer.produce(self._topic,
                                   key=result["domain_name"].encode("utf-8"),
                                   value=WorkerProcess._serialize(result))

    def _process_erroneous_df(self, partition: int, offset: int, df: pd.DataFrame, exc_info: Exception):
        try:
            keys = df["domain_name"].tolist()
            self._logger.error("Unexpected classifiers exception at partition = %s, offset = %s. Keys: %s",
                               partition, offset, str(keys), exc_info=exc_info)

            for dn in keys:
                result = {
                    "domain_name": dn,
                    "aggregate_probability": -1,
                    "aggregate_description": "",
                    "timestamp": int(datetime.now().timestamp() * 1e3),
                    "error": str(exc_info)
                }

                self._producer.produce(self._topic,
                                       key=dn.encode("utf-8"),
                                       value=WorkerProcess._serialize(result))
        except Exception as internal_e:
            self._logger.error("Unexpected error when handling an exception at partition = %s, offset = %s",
                               partition, offset, exc_info=internal_e)

    @staticmethod
    def _serialize(value: dict) -> bytes:
        return json.dumps(value, indent=None, separators=(',', ':')).encode("utf-8")


def sigterm_handler(signal_num, stack_frame):
    global process
    if process is not None:
        # noinspection PyUnresolvedReferences
        process.close()
        process = None


def init_process(config: dict, to_process: mp.Queue, processed: mp.Queue, topic_out: str, worker_id: int):
    global process
    from . import util

    util.setup_logging(config, "worker", True)
    process = WorkerProcess(config, to_process, processed, topic_out, worker_id)
    signal.signal(signal.SIGTERM, sigterm_handler)
    process.run()
