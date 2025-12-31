import json
import time
import uuid

from common.models import SOARecord, ZoneInfo, DNSRequest, ZoneRequest


def make_zone_info(zone: str = "example.com") -> ZoneInfo:
    soa = SOARecord(
        primary_ns="ns1.example.com",
        resp_mailbox_dname="hostmaster.example.com",
        serial="1",
        refresh=60,
        retry=60,
        expire=3600,
        min_ttl=300,
    )
    return ZoneInfo(
        zone=zone,
        soa=soa,
        public_suffix=zone.split(".", 1)[-1],
        has_dnskey=None,
        primary_nameserver_ips=set(),
        secondary_nameservers=set(),
        secondary_nameserver_ips=set(),
    )


def make_dns_request(zone_info: ZoneInfo | None = None) -> DNSRequest:
    return DNSRequest(zone_info=zone_info or make_zone_info())


def make_zone_request() -> ZoneRequest:
    return ZoneRequest()


def make_ip_to_process(domain: str = "example.com", ip: str = "8.8.8.8"):
    from common.models import IPToProcess

    return IPToProcess(dn=domain, ip=ip)


def write_kafka_config(tmp_path, brokers: str, app_id_prefix: str, extra_lines: list[str]):
    broker_list = [b.strip() for b in brokers.split(",") if b.strip()]
    brokers_line = "brokers = [" + ", ".join([f"\"{b}\"" for b in broker_list]) + "]"
    app_id = f"{app_id_prefix}-{uuid.uuid4()}"
    config_path = tmp_path / "config.toml"
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
            + extra_lines
        ),
        encoding="utf-8",
    )
    return config_path


def wait_for_message(consumer, matcher, timeout_s: float = 30):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        polled = consumer.poll(1.0)
        if polled is None:
            continue
        if polled.error():
            continue
        if matcher(polled):
            return polled
    return None


def decode_json(value: bytes | None):
    if value is None:
        return None
    return json.loads(value.decode("utf-8"))


def write_iana_db(tmp_path, entries: dict[str, str] | None = None):
    data = {"__last_update": 0, "com": "whois.verisign-grs.com"}
    if entries:
        data.update(entries)
    path = tmp_path / "iana_whois_servers.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path
