import multiprocessing as mp
import os
import time
import uuid

import confluent_kafka as ck
import pytest

from kafka_multiprocessor import run_client
from kafka_multiprocessor.message_processor import Message, SyncKafkaMessageProcessor


class EchoProcessor(SyncKafkaMessageProcessor[bytes, bytes]):
    def deserialize(self, message: Message[bytes, bytes]) -> None:
        message.key = message.key_raw
        message.value = message.value_raw

    def process(self, message: Message[bytes, bytes]):
        out_topic = os.getenv("KAFKA_OUTPUT_TOPIC")
        return [(out_topic, message.key, message.value)]

    def process_error(self, message: Message[bytes, bytes], error):
        return []


def _run_client(config_path: str, input_topic: str):
    os.environ["APP_CONFIG_FILE"] = config_path
    run_client(input_topic, EchoProcessor)


@pytest.mark.integration
def test_kafka_end_to_end(tmp_path):
    brokers = os.getenv("KAFKA_BROKERS")
    input_topic = os.getenv("KAFKA_INPUT_TOPIC")
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC")
    if not brokers or not input_topic or not output_topic:
        pytest.skip("KAFKA_BROKERS, KAFKA_INPUT_TOPIC, or KAFKA_OUTPUT_TOPIC not set")

    config_path = tmp_path / "config.toml"
    broker_list = [b.strip() for b in brokers.split(",") if b.strip()]
    brokers_line = "brokers = [" + ", ".join([f"\"{b}\"" for b in broker_list]) + "]"
    app_id = f"kafka-multiprocessor-test-{uuid.uuid4()}"
    config_path.write_text(
        "\n".join(
            [
                "[connection]",
                brokers_line,
                "use_ssl = false",
                "",
                "[client]",
                f'app_id = "{app_id}"',
                "workers = 1",
                "poll_timeout = 0.5",
                "init_wait = 0.0",
                "",
                "[consumer]",
                '"auto.offset.reset" = "earliest"',
                "",
                "[producer]",
                'acks = "all"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    proc = mp.Process(target=_run_client, args=(str(config_path), input_topic))
    proc.start()
    time.sleep(2.0)

    producer = ck.Producer({"bootstrap.servers": brokers})
    key = f"k-{uuid.uuid4()}".encode("utf-8")
    value = f"v-{uuid.uuid4()}".encode("utf-8")
    producer.produce(input_topic, key=key, value=value)
    producer.flush(10)

    consumer = ck.Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"test-consumer-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([output_topic])

    msg = None
    deadline = time.time() + 20
    while time.time() < deadline:
        polled = consumer.poll(1.0)
        if polled is None:
            continue
        if polled.error():
            continue
        if polled.key() == key and polled.value() == value:
            msg = polled
            break

    consumer.close()
    proc.terminate()
    proc.join(10)

    assert msg is not None
