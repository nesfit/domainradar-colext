import io
import sys

from kafka_multiprocessor.message_processor import Message, SyncKafkaMessageProcessor
from kafka_multiprocessor.standalone import InputSpec, read_input, run_cli


class StandaloneProcessor(SyncKafkaMessageProcessor[str, str]):
    def deserialize(self, message: Message[str, str]) -> None:
        message.key = (message.key_raw or b"").decode("utf-8")
        message.value = (message.value_raw or b"").decode("utf-8")

    def process(self, message: Message[str, str]):
        return [("out-topic", message.key.encode("utf-8"), message.value.encode("utf-8"))]

    def process_error(self, message: Message[str, str], error):
        return []


def test_read_input_literal_and_null(tmp_path, monkeypatch):
    assert read_input(InputSpec("literal", "hi")) == b"hi"
    assert read_input(InputSpec("null", None)) is None

    file_path = tmp_path / "value.txt"
    file_path.write_text("from-file", encoding="utf-8")
    assert read_input(InputSpec("file", str(file_path))) == b"from-file"

    monkeypatch.setattr(sys, "stdin", io.StringIO("stdin-value"))
    assert read_input(InputSpec("stdin", "-")) == b"stdin-value"


def test_run_cli_outputs(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog", "--key", "k1", "--value", "v1"])
    run_cli({}, StandaloneProcessor)
    out = capsys.readouterr().out
    assert "Output to topic 'out-topic'" in out
    assert "Key (2 bytes):" in out
    assert "Value (2 bytes):" in out
