import multiprocessing as mp
import os
import time
import uuid

import confluent_kafka as ck
import pytest

from common.util import dump_model
from kafka_multiprocessor import run_client
from thor_collectors.dns.dnscol import DNSProcessor
from tests.collector_test_utils import (
    make_dns_request,
    write_kafka_config,
    wait_for_message,
    decode_json,
)


def _run_client(config_path: str, input_topic: str):
    os.environ["APP_CONFIG_FILE"] = config_path
    run_client(input_topic, DNSProcessor, lambda cfg: cfg.get("dns", {}).get("app_id"))


@pytest.mark.integration
def test_dns_collector_end_to_end(tmp_path):
    brokers = os.getenv("KAFKA_BROKERS")
    input_topic = os.getenv("KAFKA_INPUT_TOPIC", "to_process_DNS")
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "processed_DNS")
    if not brokers:
        pytest.skip("KAFKA_BROKERS not set")
    if output_topic != "processed_DNS":
        pytest.skip("KAFKA_OUTPUT_TOPIC must be processed_DNS for DNSProcessor integration")

    config_path = write_kafka_config(
        tmp_path,
        brokers,
        "dns-collector-test",
        [
            "[dns]",
            'dns_servers = ["8.8.8.8"]',
            "timeout = 5",
            'types_to_scan = ["A"]',
            'types_to_process_IPs_from = ["A"]',
        ],
    )

    proc = mp.Process(target=_run_client, args=(str(config_path), input_topic))
    proc.start()
    time.sleep(2.0)

    request = make_dns_request()
    key = b"example.com"
    value = dump_model(request)

    producer = ck.Producer({"bootstrap.servers": brokers})
    producer.produce(input_topic, key=key, value=value)
    producer.flush(10)

    consumer = ck.Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"dns-collector-consumer-{uuid.uuid4()}",
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
