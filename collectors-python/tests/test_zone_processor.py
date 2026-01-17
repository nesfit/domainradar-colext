import pytest

import common.result_codes as rc
from common.models import ZoneResult
from common.util import dump_model
from kafka_multiprocessor.message_processor import Message
from thor_collectors.zone import zone as zone_module
from tests.collector_test_utils import make_zone_info, make_zone_request


class _DummyResolver:
    def __init__(self, zone_info: ZoneResult | None = None, error: Exception | None = None):
        self._zone_info = zone_info
        self._error = error

    async def get_zone_info(self, domain_name):
        if self._error:
            raise self._error
        return self._zone_info


@pytest.mark.asyncio
async def test_zone_processor_emits_followups(monkeypatch):
    zone_info = make_zone_info()
    resolver = _DummyResolver(zone_info=zone_info)

    def _resolver_factory(*args, **kwargs):
        return resolver

    monkeypatch.setattr(zone_module, "ZoneResolver", _resolver_factory)

    processor = zone_module.ZoneProcessor(config={})
    request = make_zone_request()
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
    assert "processed_zone" in topics
    assert "to_process_DNS" in topics
    assert "to_process_RDAP_DN" in topics


@pytest.mark.asyncio
async def test_zone_processor_respects_flags(monkeypatch):
    zone_info = make_zone_info()
    resolver = _DummyResolver(zone_info=zone_info)

    def _resolver_factory(*args, **kwargs):
        return resolver

    monkeypatch.setattr(zone_module, "ZoneResolver", _resolver_factory)

    processor = zone_module.ZoneProcessor(config={})
    request = make_zone_request()
    request.collect_dns = False
    request.collect_RDAP = False
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
    assert out[0][0] == "processed_zone"


@pytest.mark.asyncio
async def test_zone_processor_rejects_arpa():
    processor = zone_module.ZoneProcessor(config={})
    request = make_zone_request()
    message = Message(
        key_raw=b"1.0.0.127.in-addr.arpa",
        value_raw=dump_model(request),
        key="1.0.0.127.in-addr.arpa",
        value=request,
        partition=0,
        offset=0,
    )

    out = await processor.process(message)

    assert len(out) == 1
    assert out[0][0] == "processed_zone"
    result = ZoneResult.model_validate_json(out[0][2])
    assert result.status_code == rc.INVALID_DOMAIN_NAME
