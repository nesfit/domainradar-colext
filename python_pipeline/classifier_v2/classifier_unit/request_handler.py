import logging
import multiprocessing as mp
import queue
import time

from sortedcontainers import SortedDict
from .worker_process import init_process
from .types import *

import confluent_kafka as ck

# The maximum amount of entries to poll from the 'processed' queue before passing them
# forward for confirming the offsets. Used to prevent livelocks (i.e. the request handler stuck
# in accepting processed messages in get_messages_to_confirm).
MAX_CONFIRMED_MESSAGES_BATCH = 10


class RequestHandler:

    def __init__(self, config: dict, topic_in: str, topic_out: str):
        self._config = config
        self._client_config = config.get("client", {})
        self._logger = logging.getLogger(__name__)
        self._topic_in = topic_in
        self._topic_out = topic_out
        mp.set_start_method(self._client_config.get("mp_start_method", "spawn"))

        self._robs_for_partitions: ROBContainer = {}
        self._to_process = mp.Queue()  # queue of ToProcessEntry
        self._processed = mp.Queue()  # queue of ToProcessEntry
        self._liveliness_check_interval: float = self._client_config.get("liveliness_check_interval", 10.0)
        self._next_liveliness_check: float = time.time() + self._liveliness_check_interval
        self._max_entry_time_in_queue: float = self._client_config.get("max_entry_time_in_queue", 60.0)

        worker_count: int = self._client_config.get("workers", 1)
        self._logger.info("Starting worker processes (n = %s)", worker_count)
        self._workers: list[mp.Process] = []
        for i in range(worker_count):
            self._make_worker(i)

    def handle_message(self, msg: ck.Message):
        self._ensure_worker_liveliness()

        partition: int = msg.partition()
        offset: int = msg.offset()
        # noinspection PyArgumentList
        value: bytes = msg.value()

        partition_rob = self._robs_for_partitions.get(partition)
        if partition_rob is None:
            partition_rob = SortedDict()
            self._robs_for_partitions[partition] = partition_rob

        partition_rob[offset] = time.time() + self._max_entry_time_in_queue
        self._to_process.put((partition, offset, value))

    def get_messages_to_confirm(self) -> list[ck.TopicPartition] | None:
        try:
            processed = self._processed.get_nowait()
        except queue.Empty:
            return None

        livelock_prevention_counter = 0
        while processed is not None and livelock_prevention_counter < MAX_CONFIRMED_MESSAGES_BATCH:
            livelock_prevention_counter += 1
            partition_rob = self._robs_for_partitions.get(processed[0])
            if partition_rob is None:
                partition_rob = SortedDict()
                self._robs_for_partitions[processed[0]] = partition_rob

            partition_rob[processed[1]] = None
            try:
                processed = self._processed.get_nowait()
            except queue.Empty:
                break

        partition: int
        offsets: SortedDict[int, bool]
        ret = []
        for partition, offsets in self._robs_for_partitions.items():
            current_time = time.time()
            while len(offsets) > 0:
                # Extract the top of the ROB
                offset, expire_time = offsets.popitem(index=0)
                is_late = expire_time < current_time
                # 'None' means item done 
                if expire_time is None or is_late:
                    ret.append(ck.TopicPartition(self._topic_in, partition, offset + 1))
                    if is_late:
                        # TODO: a "dead letter topic"
                        self._logger.warning("Message not processed in time at partition = %s, offset = %s.",
                                             partition, offset)
                else:
                    # Put the offset back to the ROB
                    offsets[offset] = expire_time
                    break
        return ret if len(ret) > 0 else None

    def close(self) -> list[ck.TopicPartition] | None:
        process: mp.Process
        for process in self._workers:
            process.terminate()

        for process in self._workers:
            process.join(self._client_config.get("worker_kill_timeout", 1.0))

        return self.get_messages_to_confirm()

    def _make_worker(self, worker_id: int):
        worker_process = mp.Process(target=init_process,
                                    name="classifier-worker-{}".format(worker_id),
                                    args=(self._config, self._to_process, self._processed, self._topic_out, worker_id))
        self._workers.append(worker_process)
        worker_process.daemon = True
        worker_process.start()
        self._logger.info("Started worker process %s (PID %s).", worker_process.name, worker_process.pid)
        return worker_process

    def _ensure_worker_liveliness(self):
        if time.time() < self._next_liveliness_check:
            return

        dead = 0
        to_rm = []
        for process in self._workers:
            if not process.is_alive():
                dead += 1
                self._logger.warning("Worker process %s (PID %s) is dead.", process.name, process.pid)
                to_rm.append(process)

        living = len(self._workers) - dead
        for i in range(dead):
            self._workers.remove(to_rm[i])
            self._workers.append(self._make_worker(living + i))

        self._next_liveliness_check = time.time() + self._liveliness_check_interval
