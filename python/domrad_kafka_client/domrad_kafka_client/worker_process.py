import json
import logging
import multiprocessing as mp
import os
import queue
import signal
from typing import Type

from .message_processor import ProcessorBase

process = None


class WorkerProcess:
    def __init__(self, worker_id: int, config: dict, processor_type: Type[ProcessorBase],
                 to_process: mp.Queue, processed: mp.Queue):
        self._config = config
        self._logger = logging.getLogger(f"worker-{worker_id}")
        self._to_process = to_process
        self._processed = processed
        self._running = True

        self._logger.info("Initializing worker")
        self._processor = processor_type(config)
        self._processor.init()

    def run(self):
        while self._running:
            partition, offset, key, value = None, None, None, None

            # Try consuming a message
            # Watch for interrupts and errors
            try:
                partition, offset, key, value = self._to_process.get(True, 5.0)
                self._logger.debug("Processing at p=%s, o=%s", partition, offset)
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
                        ret = self._processor.process(key, value, partition, offset)
                        self._logger.debug("Processed at p=%s, o=%s", partition, offset)
                except KeyboardInterrupt:
                    self._logger.info("Interrupted. Shutting down")
                    self._running = False
                except Exception as e:
                    self._logger.error("Unexpected error. Shutting down", exc_info=e)
                    self._running = False
                finally:
                    self._processed.put((partition, offset, ret or []), True, None)
                    self._logger.debug("Placed to processed q at p=%s, o=%s", partition, offset)

        self._logger.info("Finished (PID %s)", os.getpid())

    def close(self):
        self._running = False

    @staticmethod
    def _serialize(value: dict) -> bytes:
        return json.dumps(value, indent=None, separators=(',', ':')).encode("utf-8")


def sigterm_handler(signal_num, stack_frame):
    global process
    if process is not None:
        # noinspection PyUnresolvedReferences
        process.close()
        process = None


def init_process(worker_id: int, config: dict, processor_type: Type[ProcessorBase],
                 to_process: mp.Queue, processed: mp.Queue):
    global process
    from . import util

    util.setup_logging(config, "worker", True)
    signal.signal(signal.SIGTERM, sigterm_handler)
    process = WorkerProcess(worker_id, config, processor_type, to_process, processed)
    process.run()
