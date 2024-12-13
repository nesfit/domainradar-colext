import json
import logging
import multiprocessing as mp
import os
import queue
import signal
from datetime import datetime

import pandas as pd
import pyarrow as pa

from .classifier_impl import make_classifier_impl
from .types import SimpleMessage

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

        self._logger.info("Initializing pipeline")
        self._pipeline = make_classifier_impl(config)
        self._pipeline.init()

    def run(self):
        while self._running:
            partition, offset, value = None, None, None

            # Try consuming a message
            # Watch for interrupts and errors
            try:
                partition, offset, value = self._to_process.get(True, 5.0)
                self._logger.debug("Processing at partition = %s, offset = %s", partition, offset)
            except queue.Empty:
                continue
            except KeyboardInterrupt:
                self._logger.info("Interrupted. Shutting down")
                self._running = False
            except Exception as e:
                self._logger.error("Unexpected error. Shutting down", exc_info=e)
                self._running = False

            # A message has been consumed from the input queue
            # We must ensure it gets back to the processed queue
            if partition is not None:
                ret = None
                try:
                    # Though if cancellation was requested, don't actually classify the input
                    if self._running:
                        ret = self._process_message(partition, offset, value)
                except KeyboardInterrupt:
                    self._logger.info("Interrupted. Shutting down")
                    self._running = False
                except Exception as e:
                    self._logger.error("Unexpected error. Shutting down", exc_info=e)
                    self._running = False
                finally:
                    self._processed.put((partition, offset, ret or []), True, None)

        self._logger.info("Finished (PID %s)", os.getpid())

    def close(self):
        self._running = False

    def _process_message(self, partition: int, offset: int, value: bytes) -> list[SimpleMessage]:
        ret = []

        try:
            df = pd.read_feather(pa.BufferReader(value))
        except Exception as e:
            self._logger.error("Cannot read value at partition = %s, offset = %s",
                               partition, offset, exc_info=e)
            return ret

        try:
            results = self._pipeline.classify(df)
        except Exception as e:
            return self._process_erroneous_df(partition, offset, df, e)

        result: dict
        for result in results:
            if "domain_name" not in result:
                self._logger.warning("Missing domain_name in a classification result at partition = %s, offset = %s",
                                     partition, offset)
                continue
            ret.append((result["domain_name"].encode("utf-8"), WorkerProcess._serialize(result)))

        return ret

    def _process_erroneous_df(self, partition: int, offset: int, df: pd.DataFrame,
                              exc_info: Exception) -> list[SimpleMessage]:
        ret = []

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

                ret.append((dn.encode("utf-8"), WorkerProcess._serialize(result)))
        except Exception as internal_e:
            self._logger.error("Unexpected error when handling an exception at partition = %s, offset = %s",
                               partition, offset, exc_info=internal_e)

        return ret

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
