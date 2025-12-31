import json

import pytest

import common.result_codes as rc
from common.models import IPProcessRequest
from common.util import dump_model
from kafka_multiprocessor.message_processor import Message
from thor_collectors.rtt import rtt as rtt_module
from tests.collector_test_utils import make_ip_to_process


class _PingResult:
    def __init__(self):
        self.min_rtt = 1.0
        self.avg_rtt = 2.0
        self.max_rtt = 3.0
        self.packets_sent = 3
        self.packets_received = 3
        self.jitter = 0.1


@pytest.mark.asyncio
async def test_rtt_processor_success(monkeypatch):
    async def _fake_ping(*args, **kwargs):
        return _PingResult()

    monkeypatch.setattr(rtt_module, "async_ping", _fake_ping)

    processor = rtt_module.RTTProcessor(config={})
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
    assert payload["collector"] == "rtt"
    assert payload["data"]["avg"] == 2.0


@pytest.mark.asyncio
async def test_rtt_processor_handles_unreachable(monkeypatch):
    class _FakeDestUnreachable(Exception):
        pass

    monkeypatch.setattr(rtt_module, "DestinationUnreachable", _FakeDestUnreachable)

    async def _fake_ping(*args, **kwargs):
        raise _FakeDestUnreachable("blocked")

    monkeypatch.setattr(rtt_module, "async_ping", _fake_ping)

    processor = rtt_module.RTTProcessor(config={})
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

    payload = json.loads(out[0][2].decode("utf-8"))
    assert payload["statusCode"] == rc.ICMP_DEST_UNREACHABLE


@pytest.mark.asyncio
async def test_rtt_processor_respects_collectors():
    processor = rtt_module.RTTProcessor(config={})
    ip_key = make_ip_to_process()
    request = IPProcessRequest(collectors=["rdap-ip"])
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
