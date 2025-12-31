import multiprocessing as mp
import os

import confluent_kafka as ck
import pytest

from common.util import dump_model
from kafka_multiprocessor import run_client
from thor_collectors.zone.zone import ZoneProcessor
from tests.collector_test_utils import (
    make_zone_request,
    write_kafka_config,
    wait_for_message,
    decode_json,
)


def _run_client(config_path: str, input_topic: str):
    os.environ["APP_CONFIG_FILE"] = config_path
    run_client(input_topic, ZoneProcessor)


@pytest.mark.integration
def test_zone_collector_end_to_end(tmp_path):
    brokers = os.getenv("KAFKA_BROKERS")
    input_topic = os.getenv("KAFKA_INPUT_TOPIC", "to_process_zone")
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "processed_zone")
    if not brokers:
        pytest.skip("KAFKA_BROKERS not set")
    if output_topic != "processed_zone":
        pytest.skip("KAFKA_OUTPUT_TOPIC must be processed_zone for ZoneProcessor integration")

    config_path = write_kafka_config(
        tmp_path,
        brokers,
        "zone-collector-test",
        [
            "[zone]",
            'dns_servers = ["8.8.8.8"]',
            "timeout = 5",
        ],
    )

    proc = mp.Process(target=_run_client, args=(str(config_path), input_topic))
    proc.start()

    request = make_zone_request()
    key = b"example.com"
    value = dump_model(request)

    producer = ck.Producer({"bootstrap.servers": brokers})
    producer.produce(input_topic, key=key, value=value)
    producer.flush(10)

    consumer = ck.Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": "zone-collector-consumer",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([output_topic])

    msg = wait_for_message(consumer, lambda polled: polled.key() == key)

    consumer.close()
    proc.terminate()
    proc.join(10)

    assert msg is not None
    payload = decode_json(msg.value())
    assert "statusCode" in payload
