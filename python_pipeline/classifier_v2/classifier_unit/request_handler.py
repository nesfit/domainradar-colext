import logging
import multiprocessing as mp
import os
import queue
import signal
import time

from confluent_kafka import KafkaException, KafkaError

from .worker_process import init_process
from .types import *

import confluent_kafka as ck


class RequestHandler:

    def __init__(self, config: dict, topic_in: str, topic_out: str, consumer: ck.Consumer, producer: ck.Producer):
        self._config = config
        self._client_config = config.get("client", {})
        self._logger = logging.getLogger("req-handler")
        self._topic_in = topic_in
        self._topic_out = topic_out
        self._consumer = consumer
        self._producer = producer
        mp.set_start_method(self._client_config.get("mp_start_method", "forkserver"))

        self._robs_for_partitions: ROBContainer = {}
        self._in_flight = 0
        self._locked = False
        self._to_process = mp.Queue()  # queue of ToProcessEntry
        self._processed = mp.Queue()  # queue of ProcessedEntry
        self._liveliness_check_interval: float = self._client_config.get("liveliness_check_interval", 10.0)
        self._next_liveliness_check: float = time.time() + self._liveliness_check_interval
        self._entry_late_warning_threshold: float = self._client_config.get("entry_late_warning_threshold", 30.0)
        self._max_entry_time_in_queue: float = self._client_config.get("max_entry_time_in_queue", 120.0)
        self._max_queued_items: int = self._client_config.get("max_queued_items", 100)
        self._resume_after_freed_items: int = self._client_config.get("resume_after_freed_items", 10)
        self._max_entries_to_confirm: int = self._client_config.get("max_entries_to_confirm", 50)
        self._closing = False

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

        partition_rob[offset] = time.time() + self._entry_late_warning_threshold
        self._in_flight += 1
        self._to_process.put((partition, offset, value))

    def handle_results(self):
        self._update_rob()
        self._produce_results()

    def can_continue(self) -> bool:
        if self._locked:
            if self._in_flight < (self._max_queued_items - self._resume_after_freed_items):
                self._locked = False
                self._logger.debug("Resumed from in-flight limit wait")
                return True
            return False
        else:
            if self._in_flight < self._max_queued_items:
                return True
            else:
                self._locked = True
                self._logger.debug("Reached in-flight limit")
                return False

    def _update_rob(self) -> None:
        try:
            processed: ProcessedEntry = self._processed.get_nowait()
        except queue.Empty:
            return None

        livelock_prevention_counter = 0
        while processed is not None and livelock_prevention_counter < self._max_entries_to_confirm:
            livelock_prevention_counter += 1
            partition_rob = self._robs_for_partitions.get(processed[0])
            if partition_rob is None:
                partition_rob = SortedDict()
                self._robs_for_partitions[processed[0]] = partition_rob

            partition_rob[processed[1]] = processed[2]
            try:
                processed = self._processed.get_nowait()
            except queue.Empty:
                break

    def _produce_results(self) -> None:
        partition: int
        offsets: ROB

        for partition, offsets in self._robs_for_partitions.items():
            current_time = time.time()
            while len(offsets) > 0:
                # Extract the top of the ROB
                offset: Offset
                expire_time_or_result: float | list[SimpleMessage]
                offset, expire_time_or_result = offsets.peekitem(index=0)
                # 'None' means item done
                item_done = not isinstance(expire_time_or_result, float)
                if item_done:
                    self._producer.begin_transaction()
                    for result_msg in expire_time_or_result:
                        self._producer.produce(self._topic_out, key=result_msg[0], value=result_msg[1])

                    positions = [ck.TopicPartition(self._topic_in, partition, offset=(offset + 1))]
                    self._producer.send_offsets_to_transaction(positions, self._consumer.consumer_group_metadata())

                    try:
                        self._producer.commit_transaction()
                        offsets.popitem(index=0)
                        self._in_flight -= 1
                    except KafkaException as e:
                        self._logger.error("Error commiting transaction for p=%s, o=%s",
                                           partition, offset, exc_info=e)

                        kafka_error_code: int = e.args[0].code()
                        if kafka_error_code == KafkaError.PRODUCER_FENCED \
                                or kafka_error_code == KafkaError.OUT_OF_ORDER_SEQUENCE_NUMBER:
                            raise e
                        else:
                            self._producer.abort_transaction()
                            # Next iteration will try again
                else:
                    if not item_done and expire_time_or_result < current_time and not self._closing:
                        self._logger.info("Entry taking too long at p=%s, o=%s", partition, offset)

                        if (current_time - expire_time_or_result) > self._max_entry_time_in_queue:
                            self._logger.error("Commiting suicide as an entry took over %s s in queue",
                                               self._max_entry_time_in_queue)
                            os.kill(os.getpid(), signal.SIGINT)
                            return
                    break

    def close(self) -> None:
        process: mp.Process
        for process in self._workers:
            self._logger.debug("Terminating %s", process.pid)
            os.kill(process.pid, signal.SIGINT)

        for process in self._workers:
            # self._logger.debug("Waiting for %s", process.pid)
            process.join(self._client_config.get("worker_kill_timeout", 1.0))
            self._logger.debug("%s ended", process.pid)

        self._closing = True
        self.handle_results()

    def _make_worker(self, worker_id: int):
        worker_process = mp.Process(target=init_process,
                                    name="classifier-worker-{}".format(worker_id),
                                    args=(self._config, self._to_process, self._processed, self._topic_out, worker_id))
        self._workers.append(worker_process)
        worker_process.daemon = True
        worker_process.start()
        self._logger.info("Started worker process %s (PID %s)", worker_process.name, worker_process.pid)
        return worker_process

    def _ensure_worker_liveliness(self):
        if time.time() < self._next_liveliness_check:
            return

        new_names = []
        to_rm = []
        for process in self._workers:
            if not process.is_alive():
                self._logger.warning("Worker process %s (PID %s) is dead", process.name, process.pid)
                to_rm.append(process)
                new_names.append(process.name)

        dead = len(to_rm)
        for i in range(dead):
            self._workers.remove(to_rm[i])
            adopted_worker_id = int(new_names[i].split('-')[-1])
            self._workers.append(self._make_worker(adopted_worker_id))

        self._next_liveliness_check = time.time() + self._liveliness_check_interval
