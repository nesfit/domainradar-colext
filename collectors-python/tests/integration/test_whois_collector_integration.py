import multiprocessing as mp
import os

import confluent_kafka as ck
import pytest

from common.util import dump_model
from kafka_multiprocessor import run_client
from common.models import WHOISRequest
from thor_collectors.whois.whois import WHOISProcessor
from tests.collector_test_utils import (
    write_kafka_config,
    wait_for_message,
    decode_json,
    write_iana_db,
)


def _run_client(config_path: str, input_topic: str):
    os.environ["APP_CONFIG_FILE"] = config_path
    run_client(input_topic, WHOISProcessor)


@pytest.mark.integration
def test_whois_collector_end_to_end(tmp_path):
    brokers = os.getenv("KAFKA_BROKERS")
    input_topic = os.getenv("KAFKA_INPUT_TOPIC", "to_process_WHOIS")
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "processed_WHOIS")
    if not brokers:
        pytest.skip("KAFKA_BROKERS not set")
    if output_topic != "processed_WHOIS":
        pytest.skip("KAFKA_OUTPUT_TOPIC must be processed_WHOIS for WHOISProcessor integration")

    iana_db_path = write_iana_db(tmp_path)

    config_path = write_kafka_config(
        tmp_path,
        brokers,
        "whois-collector-test",
        [
            "[whois]",
            f'iana_db_path = "{iana_db_path}"',
            "timeout = 2.0",
        ],
    )

    proc = mp.Process(target=_run_client, args=(str(config_path), input_topic))
    proc.start()

    request = WHOISRequest()
    key = b"example.invalid"
    value = dump_model(request)

    producer = ck.Producer({"bootstrap.servers": brokers})
    producer.produce(input_topic, key=key, value=value)
    producer.flush(10)

    consumer = ck.Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": "whois-collector-consumer",
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
