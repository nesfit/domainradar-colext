import multiprocessing as mp
import os

import confluent_kafka as ck
import pytest

from common.util import dump_model
from kafka_multiprocessor import run_client
from thor_collectors.rtt.rtt import RTTProcessor
from tests.collector_test_utils import make_ip_to_process, write_kafka_config, wait_for_message, decode_json


def _run_client(config_path: str, input_topic: str):
    os.environ["APP_CONFIG_FILE"] = config_path
    run_client(input_topic, RTTProcessor)


@pytest.mark.integration
def test_rtt_collector_end_to_end(tmp_path):
    brokers = os.getenv("KAFKA_BROKERS")
    input_topic = os.getenv("KAFKA_INPUT_TOPIC", "to_process_IP")
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "collected_IP_data")
    if not brokers:
        pytest.skip("KAFKA_BROKERS not set")
    if not os.getenv("TESTS_ALLOW_NETWORK"):
        pytest.skip("TESTS_ALLOW_NETWORK not set")
    if output_topic != "collected_IP_data":
        pytest.skip("KAFKA_OUTPUT_TOPIC must be collected_IP_data for RTT integration")

    config_path = write_kafka_config(
        tmp_path,
        brokers,
        "rtt-collector-test",
        [
            "[rtt]",
            "ping_count = 1",
            "privileged = false",
            "timeout_ms = 1000",
            "interval_ms = 1000",
        ],
    )

    proc = mp.Process(target=_run_client, args=(str(config_path), input_topic))
    proc.start()

    ip_key = make_ip_to_process(ip="8.8.8.8")
    key = dump_model(ip_key)

    producer = ck.Producer({"bootstrap.servers": brokers})
    producer.produce(input_topic, key=key, value=None)
    producer.flush(10)

    consumer = ck.Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": "rtt-collector-consumer",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([output_topic])

    msg = wait_for_message(consumer, lambda polled: polled.key() == key, timeout_s=60)

    consumer.close()
    proc.terminate()
    proc.join(10)

    assert msg is not None
    payload = decode_json(msg.value())
    assert "statusCode" in payload
