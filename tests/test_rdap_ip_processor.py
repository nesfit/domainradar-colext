import json

import pytest

import common.result_codes as rc
from common.models import IPProcessRequest
from common.util import dump_model
from kafka_multiprocessor.message_processor import Message
from thor_collectors.rdap_ip import rdap_ip as rdap_ip_module
from tests.collector_test_utils import make_ip_to_process


class _DummyRDAPResponse:
    def __init__(self, payload):
        self._payload = payload

    def to_dict(self):
        return self._payload


@pytest.mark.asyncio
async def test_rdap_ip_processor_success(monkeypatch):
    async def _fetch_ip(self, dn, address):
        return _DummyRDAPResponse({"ip": address}), rc.OK, None

    monkeypatch.setattr(rdap_ip_module.RDAPIPProcessor, "fetch_ip", _fetch_ip)

    processor = rdap_ip_module.RDAPIPProcessor(config={})
    ip_key = make_ip_to_process()
    message = Message(
        key_raw=dump_model(ip_key),
        value_raw=None,
        key=ip_key,
        value=None,
        partition=0,
        offset=0,
    )

    out = await processor.process(message)

    assert len(out) == 1
    assert out[0][0] == "collected_IP_data"
    payload = json.loads(out[0][2].decode("utf-8"))
    assert payload["statusCode"] == rc.OK
    assert payload["collector"] == "rdap-ip"
    assert payload["data"]["ip"] == "8.8.8.8"


@pytest.mark.asyncio
async def test_rdap_ip_processor_respects_collectors():
    processor = rdap_ip_module.RDAPIPProcessor(config={})
    ip_key = make_ip_to_process()
    request = IPProcessRequest(collectors=["rtt"])
    message = Message(
        key_raw=dump_model(ip_key),
        value_raw=dump_model(request),
        key=ip_key,
        value=request,
        partition=0,
        offset=0,
    )

    out = await processor.process(message)

    assert out == []


def test_rdap_ip_bucket_key_ipv4_and_ipv6():
    processor = rdap_ip_module.RDAPIPProcessor(config={})

    class _Client:
        def __init__(self, value):
            self._value = value

        def _get_rdap_server(self, ip):
            return self._value

    processor._ipv4_client = _Client("https://rdap.ipv4.test")
    processor._ipv6_client = _Client("https://rdap.ipv6.test")

    ipv4_key = make_ip_to_process(ip="1.2.3.4")
    ipv6_key = make_ip_to_process(ip="2001:db8::1")

    msg_v4 = Message(None, None, ipv4_key, None, 0, 0)
    msg_v6 = Message(None, None, ipv6_key, None, 0, 0)

    assert processor.get_rl_bucket_key(msg_v4) == "https://rdap.ipv4.test"
    assert processor.get_rl_bucket_key(msg_v6) == "https://rdap.ipv6.test"
