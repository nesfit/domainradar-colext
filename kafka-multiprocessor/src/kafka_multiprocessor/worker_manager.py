import logging
import multiprocessing as mp
import os
import queue
import signal
import time
from typing import Type

from confluent_kafka import KafkaException, KafkaError

from .worker_process import init_process
from .message_processor import KafkaMessageProcessor
from .types import *
from .util import get_worker_logger_config

import confluent_kafka as ck


class WorkerManager:

    def __init__(self, config: dict, topic_in: str, processor_type: Type[KafkaMessageProcessor],
                 consumer: ck.Consumer, producer: ck.Producer):
        self._config = config
        self._topic_in = topic_in
        self._processor_type = processor_type
        self._consumer = consumer
        self._producer = producer

        self._client_config = config.get("client", {})
        self._logger = logging.getLogger("req-handler")

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
        key: bytes = msg.key()
        # noinspection PyArgumentList
        value: bytes = msg.value()

        self._logger.trace("Handling incoming message p=%s, o=%s", partition, offset)
        partition_rob = self._robs_for_partitions.get(partition)
        if partition_rob is None:
            self._logger.trace("Creating new ROB for p=%s", partition)
            partition_rob = SortedDict()
            self._robs_for_partitions[partition] = partition_rob

        late_warning = time.time() + self._entry_late_warning_threshold
        partition_rob[offset] = late_warning
        self._in_flight += 1
        self._logger.trace("Late warning for p=%s, o=%s at %s; in_flight=%s; adding to_process entry",
                           partition, offset, late_warning, self._in_flight)
        self._to_process.put((partition, offset, key, value))

    def handle_results(self):
        self._update_rob()
        self._produce_results()

    def can_continue(self) -> bool:
        if self._locked:
            if self._in_flight < (self._max_queued_items - self._resume_after_freed_items):
                self._locked = False
                self._logger.trace("Resumed from in-flight limit wait")
                return True
            self._logger.trace("Still locked, in_flight=%s", self._in_flight)
            return False
        else:
            if self._in_flight < self._max_queued_items:
                return True
            else:
                self._locked = True
                self._logger.trace("Reached in-flight limit, in_flight=%s", self._in_flight)
                return False

    def _update_rob(self) -> None:
        self._logger.trace("Updating ROB: accepting from processed queue")
        livelock_prevention_counter = 0
        while livelock_prevention_counter < self._max_entries_to_confirm:
            try:
                processed: ProcessedEntry = self._processed.get_nowait()
            except queue.Empty:
                self._logger.trace("Processed queue empty at ctr=%s", livelock_prevention_counter)
                break

            self._logger.trace("Accepted item with %s messages at ctr=%s, p=%s, o=%s", len(processed[2]),
                               livelock_prevention_counter, processed[0], processed[1])
            partition_rob = self._robs_for_partitions.get(processed[0])
            if partition_rob is None:
                self._logger.trace("Partition ROB for %s does not exist, weird!", processed[0])
                partition_rob = SortedDict()
                self._robs_for_partitions[processed[0]] = partition_rob

            partition_rob[processed[1]] = processed[2]
            livelock_prevention_counter += 1

    def _produce_results(self) -> None:
        partition: int
        offsets: ROB

        for partition, offsets in self._robs_for_partitions.items():
            current_time = time.time()
            self._logger.trace("Evaluating ROB for p=%s at t=%s; offsets in ROB: %s", partition, current_time,
                               len(offsets))
            while len(offsets) > 0:
                # Extract the top of the ROB
                offset: Offset
                expire_time_or_result: float | list[SimpleMessage]
                offset, expire_time_or_result = offsets.peekitem(index=0)
                # 'None' means item done
                item_done = not isinstance(expire_time_or_result, float)

                if item_done:
                    self._logger.trace("Top of ROB p=%s is o=%s, done with %s msgs", partition, offset,
                                       len(expire_time_or_result))

                    self._producer.begin_transaction()
                    for result_msg in expire_time_or_result:
                        self._producer.produce(result_msg[0], key=result_msg[1], value=result_msg[2])

                    positions = [ck.TopicPartition(self._topic_in, partition, offset=(offset + 1))]
                    self._producer.send_offsets_to_transaction(positions, self._consumer.consumer_group_metadata())

                    try:
                        self._logger.trace("Commiting transaction")
                        self._producer.commit_transaction()
                        offsets.popitem(index=0)
                        self._in_flight -= 1
                        self._logger.trace("Popped offset, %s offsets remaining; in_flight=%s", len(offsets),
                                           self._in_flight)
                    except KafkaException as e:
                        self._logger.error("Error commiting transaction for p=%s, o=%s",
                                           partition, offset, exc_info=e)

                        kafka_error_code: int = e.args[0].code()
                        if kafka_error_code == KafkaError.PRODUCER_FENCED \
                                or kafka_error_code == KafkaError.OUT_OF_ORDER_SEQUENCE_NUMBER:
                            self._logger.trace("Producer fenced or out of order, crashing")
                            raise e
                        else:
                            self._logger.trace("Aborting transaction, will retry")
                            self._producer.abort_transaction()
                            # Next iteration will try again
                else:
                    self._logger.trace("Top of ROB p=%s is o=%s, waiting with exp_t=%s", partition, offset,
                                       expire_time_or_result)

                    if not item_done and expire_time_or_result < current_time and not self._closing:
                        self._logger.info("Entry taking too long at p=%s, o=%s", partition, offset)

                        if (current_time - expire_time_or_result) > self._max_entry_time_in_queue:
                            self._logger.error("Commiting suicide as an entry took over %s s in queue",
                                               self._max_entry_time_in_queue)
                            os.kill(os.getpid(), signal.SIGINT)
                            return

                    self._logger.trace("Breaking loop for ROB p=%s; %s offset remaining", partition, len(offsets))
                    break

    def close(self) -> None:
        self._logger.trace("Closing the WorkerManager")
        process: mp.Process
        for process in self._workers:
            self._logger.debug("Terminating %s", process.pid)
            try:
                os.kill(process.pid, signal.SIGINT)
            except ProcessLookupError:
                self._logger.debug("ProcessLookupError for %s - process already dead?", process.pid)
                pass

        for process in self._workers:
            self._logger.trace("Waiting for %s", process.pid)
            process.join(self._client_config.get("worker_kill_timeout", 1.0))
            self._logger.debug("%s ended", process.pid)

        self._logger.trace("Activating closing mode, calling handle_results")
        self._closing = True
        self.handle_results()

    def _make_worker(self, worker_id: int):
        self._logger.trace("Making worker worker_id=%s", worker_id)
        worker_process = mp.Process(target=init_process,
                                    name="worker-{}".format(worker_id),
                                    args=(worker_id, self._config, self._processor_type,
                                          self._to_process, self._processed, get_worker_logger_config()))
        self._workers.append(worker_process)
        worker_process.daemon = True
        worker_process.start()
        self._logger.info("Started worker process %s (PID %s)", worker_process.name, worker_process.pid)
        return worker_process

    def _ensure_worker_liveliness(self):
        if time.time() < self._next_liveliness_check:
            return

        self._logger.trace("Checking if workers live")
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
