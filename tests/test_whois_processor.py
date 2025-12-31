import json

import pytest

import common.result_codes as rc
from common.models import WHOISRequest
from common.util import dump_model
from kafka_multiprocessor.message_processor import Message
from thor_collectors.whois import whois as whois_module
from thor_collectors.whois.client.iana_db import IanaDBGenerator


@pytest.fixture
def whois_processor(monkeypatch):
    def _fake_get_db(self):
        return {"__last_update": 0, "com": "whois.example"}

    monkeypatch.setattr(IanaDBGenerator, "get_db", _fake_get_db)
    return whois_module.WHOISProcessor(config={})


@pytest.mark.asyncio
async def test_whois_processor_uses_zone(monkeypatch, whois_processor):
    calls = []

    async def _fetch_whois(self, domain_name, endpoint):
        calls.append((domain_name, endpoint))
        return "ok", rc.OK, None

    monkeypatch.setattr(whois_module.WHOISProcessor, "fetch_whois", _fetch_whois)

    request = WHOISRequest(zone="example.com")
    message = Message(
        key_raw=b"sub.example.com",
        value_raw=dump_model(request),
        key="sub.example.com",
        value=request,
        partition=0,
        offset=0,
        custom_data=("com", "whois.example"),
    )

    out = await whois_processor.process(message)

    assert len(out) == 1
    assert calls[0][0] == "example.com"
    payload = json.loads(out[0][2].decode("utf-8"))
    assert payload["whoisTarget"] == "example.com"


@pytest.mark.asyncio
async def test_whois_processor_retries_registered_domain(monkeypatch, whois_processor):
    calls = []

    async def _fetch_whois(self, domain_name, endpoint):
        calls.append(domain_name)
        if len(calls) == 1:
            return None, rc.NOT_FOUND, None
        return "ok", rc.OK, None

    monkeypatch.setattr(whois_module.WHOISProcessor, "fetch_whois", _fetch_whois)

    request = WHOISRequest()
    message = Message(
        key_raw=b"sub.example.com",
        value_raw=dump_model(request),
        key="sub.example.com",
        value=request,
        partition=0,
        offset=0,
        custom_data=("com", "whois.example"),
    )

    out = await whois_processor.process(message)

    assert len(out) == 1
    assert calls == ["sub.example.com", "example.com"]
