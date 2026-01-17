import json

import pytest

import common.result_codes as rc
from common.models import DNSResult, DNSData, CNAMERecord, IPFromRecord
from common.util import dump_model
from kafka_multiprocessor.message_processor import Message
from thor_collectors.dns import dnscol
from tests.collector_test_utils import make_dns_request


class _DummyScanner:
    def __init__(self, result: DNSResult):
        self._result = result

    async def scan_dns(self, domain_name, request):
        return self._result


@pytest.mark.asyncio
async def test_dns_processor_emits_followups(monkeypatch):
    dns_data = DNSData(
        cname=CNAMERecord(value="alias.example.com", related_ips={"9.9.9.9"}),
        a={"1.1.1.1"},
    )
    result = DNSResult(
        status_code=rc.OK,
        dns_data=dns_data,
        ips=[IPFromRecord(ip="2.2.2.2", type="A")],
    )

    def _scanner_factory(*args, **kwargs):
        return _DummyScanner(result)

    monkeypatch.setattr(dnscol, "DNSScanner", _scanner_factory)

    processor = dnscol.DNSProcessor(config={})
    request = make_dns_request()
    message = Message(
        key_raw=b"example.com",
        value_raw=dump_model(request),
        key="example.com",
        value=request,
        partition=0,
        offset=0,
    )

    out = await processor.process(message)

    topics = [item[0] for item in out]
    assert "processed_DNS" in topics
    assert "to_process_IP" in topics
    assert "to_process_TLS" in topics

    ip_msg = next(item for item in out if item[0] == "to_process_IP")
    ip_payload = json.loads(ip_msg[1].decode("utf-8"))
    assert ip_payload["dn"] == "example.com"
    assert ip_payload["ip"] == "2.2.2.2"

    tls_msg = next(item for item in out if item[0] == "to_process_TLS")
    assert tls_msg[2] == b"9.9.9.9"


@pytest.mark.asyncio
async def test_dns_processor_no_followups_on_error(monkeypatch):
    result = DNSResult(status_code=rc.TIMEOUT, error="timeout")

    def _scanner_factory(*args, **kwargs):
        return _DummyScanner(result)

    monkeypatch.setattr(dnscol, "DNSScanner", _scanner_factory)

    processor = dnscol.DNSProcessor(config={})
    request = make_dns_request()
    message = Message(
        key_raw=b"example.com",
        value_raw=dump_model(request),
        key="example.com",
        value=request,
        partition=0,
        offset=0,
    )

    out = await processor.process(message)

    assert len(out) == 1
    assert out[0][0] == "processed_DNS"


def test_get_ip_for_tls_prefers_cname_then_a_then_aaaa():
    data = DNSData(cname=CNAMERecord(value="alias.example.com", related_ips={"9.9.9.9"}))
    assert dnscol.DNSProcessor.get_ip_for_tls(data) == "9.9.9.9"

    data = DNSData(a={"1.1.1.1"}, aaaa={"2001:db8::1"})
    assert dnscol.DNSProcessor.get_ip_for_tls(data) == "1.1.1.1"

    data = DNSData(aaaa={"2001:db8::1"})
    assert dnscol.DNSProcessor.get_ip_for_tls(data) == "2001:db8::1"
