import queue
import time

import confluent_kafka as ck

from kafka_multiprocessor.worker_manager import WorkerManager


class DummyProcessor:
    pass


class FakeMessage:
    def __init__(self, partition: int, offset: int, key: bytes, value: bytes):
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value


class FakeConsumer:
    def consumer_group_metadata(self):
        return "metadata"


class FakeProducer:
    def __init__(self):
        self.produced = []
        self.commits = []
        self.offsets = []
        self.begun = 0
        self.aborted = 0

    def begin_transaction(self):
        self.begun += 1

    def produce(self, topic, key=None, value=None):
        self.produced.append((topic, key, value))

    def send_offsets_to_transaction(self, positions, metadata):
        self.offsets.append((positions, metadata))

    def commit_transaction(self):
        self.commits.append(time.time())

    def abort_transaction(self):
        self.aborted += 1


def _make_manager():
    config = {
        "client": {
            "workers": 0,
            "max_queued_items": 2,
            "resume_after_freed_items": 1,
            "entry_late_warning_threshold": 5.0,
            "max_entry_time_in_queue": 10.0,
        }
    }
    manager = WorkerManager(
        config=config,
        topic_in="input-topic",
        processor_type=DummyProcessor,
        consumer=FakeConsumer(),
        producer=FakeProducer(),
    )
    manager._to_process = queue.Queue()
    manager._processed = queue.Queue()
    return manager


def test_reorder_buffer_commit_order():
    manager = _make_manager()
    producer = manager._producer

    msg0 = FakeMessage(0, 0, b"k0", b"v0")
    msg1 = FakeMessage(0, 1, b"k1", b"v1")
    manager.handle_message(msg0)
    manager.handle_message(msg1)

    manager._processed.put((0, 1, [("out", b"k1", b"v1")]))
    manager.handle_results()
    assert producer.produced == []

    manager._processed.put((0, 0, [("out", b"k0", b"v0")]))
    manager.handle_results()

    assert producer.produced == [("out", b"k0", b"v0"), ("out", b"k1", b"v1")]
    assert manager._in_flight == 0
    assert len(producer.commits) == 2
    assert len(producer.offsets) == 2
    assert isinstance(producer.offsets[0][0][0], ck.TopicPartition)


def test_can_continue_locking():
    manager = _make_manager()
    manager._in_flight = 2

    assert manager.can_continue() is False
    manager._in_flight = 0
    assert manager.can_continue() is True
