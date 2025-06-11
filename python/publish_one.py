#!/usr/bin/env python3

import argparse
import datetime
import json
import tomllib
from time import sleep

from confluent_kafka import Producer


def make_client_settings(config: dict) -> dict:
    ret = {
        "bootstrap.servers": ",".join(config["connection"]["brokers"]),
    }

    if config["connection"]["use_ssl"]:
        ret["security.protocol"] = "SSL"
        ret["ssl.ca.location"] = config["connection"]["ssl"]["ca_file"]
        ret["ssl.certificate.location"] = config["connection"]["ssl"]["client_cert_file"]
        ret["ssl.key.location"] = config["connection"]["ssl"]["client_key_file"]
        ret["ssl.key.password"] = config["connection"]["ssl"]["client_key_password"]
        ret["ssl.endpoint.identification.algorithm"] = \
            "https" if config["connection"]["ssl"]["check_hostname"] else "none"

    return ret


def make_producer_settings(config: dict) -> dict:
    ret = make_client_settings(config)
    ret = ret | config.get("producer", {})

    return ret


def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    # else: delivery succeeded; do nothing


def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def dumps_tight(obj):
    """Serialize an object to a JSON string without extra spaces."""
    return json.dumps(obj, indent=None, separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser(
        description="Publish JSON subsections to Kafka topics"
    )
    parser.add_argument("config", help="Path to configuration file")
    parser.add_argument("domain_name", help="Domain name to use as message key")
    parser.add_argument("data_file", help="Path to JSON input file")
    args = parser.parse_args()

    domain = args.domain_name
    data = load_json(args.data_file)

    # Configure Kafka producer
    with open(args.config, "rb") as f:
        config = tomllib.load(f)

    producer = Producer(make_producer_settings(config))

    # Mapping from JSON field to Kafka topic
    section_topics = {
        "zone": "processed_zone",
        "dnsResult": "processed_DNS",
        "tlsResult": "processed_TLS",
        "rdapDomainResult": "processed_RDAP_DN",
    }

    # Transform the zone data into a result
    if "zone" in data:
        current_timestamp = int(datetime.datetime.now().timestamp() * 1000)
        data["zone"] = {
            "statusCode": 0,
            "error": None,
            "lastAttempt": data.get("dnsResult", {}).get("lastAttempt", current_timestamp) - 500,
            "zone": data["zone"]
        }

    # Publish top-level sections
    for section, topic in section_topics.items():
        payload = data.get(section)
        if payload is not None:
            producer.produce(
                topic=topic,
                key=domain,
                value=dumps_tight(payload),
                callback=delivery_report
            )
            producer.poll(0)
            print(f"Published {section} data to {topic}")
            sleep(0.4)

    # Publish IP-based collected data
    ip_results = data.get("ipResults", {})
    for ip, collectors in ip_results.items():
        for collector_name, result in collectors.items():
            key_obj = {"dn": domain, "ip": ip}
            producer.produce(
                topic="collected_IP_data",
                key=dumps_tight(key_obj),
                value=dumps_tight(result),
                callback=delivery_report
            )
            producer.poll(0)
            print(f"Published {collector_name} data for {ip} to collected_IP_data")
            sleep(0.3)

    # Ensure all messages are delivered
    producer.flush()


if __name__ == "__main__":
    main()
