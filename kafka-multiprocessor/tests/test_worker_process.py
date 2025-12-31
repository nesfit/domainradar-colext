import pytest

from kafka_multiprocessor.message_processor import Message, SyncKafkaMessageProcessor
from kafka_multiprocessor.worker_process import WorkerProcess, _LoopError


class SuccessProcessor(SyncKafkaMessageProcessor[str, str]):
    def deserialize(self, message: Message[str, str]) -> None:
        message.key = (message.key_raw or b"").decode("utf-8")
        message.value = (message.value_raw or b"").decode("utf-8")

    def process(self, message: Message[str, str]):
        return [("out", message.key.encode("utf-8"), message.value.encode("utf-8"))]

    def process_error(self, message: Message[str, str], error):
        return []


class ErrorProcessor(SyncKafkaMessageProcessor[str, str]):
    def deserialize(self, message: Message[str, str]) -> None:
        message.key = "k"
        message.value = "v"

    def process(self, message: Message[str, str]):
        raise ValueError("boom")

    def process_error(self, message: Message[str, str], error):
        assert isinstance(error, Exception)
        return [("err", b"k", b"v")]


def test_worker_process_process_success():
    worker = WorkerProcess(
        worker_id=1,
        config={"client": {}},
        processor_type=SuccessProcessor,
        to_process=None,
        processed=None,
    )
    result = worker._process(b"k", b"v", 0, 0)
    assert result == [("out", b"k", b"v")]


def test_worker_process_process_error():
    worker = WorkerProcess(
        worker_id=1,
        config={"client": {}},
        processor_type=ErrorProcessor,
        to_process=None,
        processed=None,
    )
    with pytest.raises(_LoopError) as exc:
        worker._process(b"k", b"v", 0, 0)
    assert exc.value.error_messages_to_send == [("err", b"k", b"v")]
