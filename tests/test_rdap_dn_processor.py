import json

import pytest

import common.result_codes as rc
from common.models import RDAPDomainRequest
from common.util import dump_model
from kafka_multiprocessor.message_processor import Message
from thor_collectors.rdap_dn import rdap_dn as rdap_dn_module


class _DummyRDAPResponse:
    def __init__(self, payload):
        self._payload = payload

    def to_dict(self):
        return self._payload


class _DummyEntity:
    def __init__(self, payload):
        self._payload = payload

    def to_dict(self):
        return self._payload


@pytest.mark.asyncio
async def test_rdap_dn_processor_success(monkeypatch):
    async def _fetch_rdap(self, target):
        return _DummyRDAPResponse({"name": target}), [_DummyEntity({"role": "registrant"})], 0, None

    monkeypatch.setattr(rdap_dn_module.RDAPDNProcessor, "fetch_rdap", _fetch_rdap)

    processor = rdap_dn_module.RDAPDNProcessor(config={})
    request = RDAPDomainRequest()
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
    assert out[0][0] == "processed_RDAP_DN"
    payload = json.loads(out[0][2].decode("utf-8"))
    assert payload["statusCode"] == rc.OK
    assert payload["rdapTarget"] == "example.com"
    assert payload["rdapData"]["name"] == "example.com"
    assert payload["entities"][0]["role"] == "registrant"


@pytest.mark.asyncio
async def test_rdap_dn_processor_emits_whois_on_error(monkeypatch):
    async def _fetch_rdap(self, target):
        return None, None, rc.NOT_FOUND, None

    monkeypatch.setattr(rdap_dn_module.RDAPDNProcessor, "fetch_rdap", _fetch_rdap)

    processor = rdap_dn_module.RDAPDNProcessor(config={})
    request = RDAPDomainRequest()
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
    assert "processed_RDAP_DN" in topics
    assert "to_process_WHOIS" in topics


@pytest.mark.asyncio
async def test_rdap_dn_processor_retries_registered_domain(monkeypatch):
    calls = []

    async def _fetch_rdap(self, target):
        calls.append(target)
        if len(calls) == 1:
            return None, None, rc.NOT_FOUND, None
        return _DummyRDAPResponse({"name": target}), [], rc.OK, None

    monkeypatch.setattr(rdap_dn_module.RDAPDNProcessor, "fetch_rdap", _fetch_rdap)

    processor = rdap_dn_module.RDAPDNProcessor(config={})
    request = RDAPDomainRequest()
    message = Message(
        key_raw=b"sub.example.com",
        value_raw=dump_model(request),
        key="sub.example.com",
        value=request,
        partition=0,
        offset=0,
    )

    await processor.process(message)

    assert calls == ["sub.example.com", "example.com"]
