import os

import pytest

from thor_collectors.whois.client import WhoisClient


@pytest.mark.integration
def test_whois_query_network():
    if not os.getenv("WHOIS_NETWORK_TEST"):
        pytest.skip("WHOIS_NETWORK_TEST not set")

    client = WhoisClient()
    response = client.query("some-random-domain.com", "whois.iana.org", timeout=5.0)

    assert "refer" in response.lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_whois_query_async_network():
    if not os.getenv("WHOIS_NETWORK_TEST"):
        pytest.skip("WHOIS_NETWORK_TEST not set")

    client = WhoisClient()
    response = await client.query_async("some-random-domain.com", "whois.iana.org", timeout=5.0)

    assert "refer" in response.lower()
