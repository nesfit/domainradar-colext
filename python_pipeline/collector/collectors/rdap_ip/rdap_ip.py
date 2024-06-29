import asyncio
import ipaddress

import httpx
import whodap
from whodap import IPv4Client, IPv6Client
from whodap.errors import *
from whodap.response import IPv4Response, IPv6Response

import common.result_codes as rc
from collectors.limiter import LimiterProvider
from collectors.util import (make_rdap_ssl_context, should_omit_ip,
                             handle_top_level_component_exception, get_ip_safe)
from common import read_config, make_app
from common.audit import log_unhandled_error
from common.models import IPToProcess, IPProcessRequest, RDAPIPResult
from common.util import ensure_model

COLLECTOR = "rdap_ip"

# Read the config
config = read_config()
component_config = config.get(COLLECTOR, {})

HTTP_TIMEOUT = component_config.get("http_timeout", 5)
CONCURRENCY = component_config.get("concurrency", 4)

# The Faust application
rdap_ip_app = make_app(COLLECTOR, config)

# The input and output topics
topic_to_process = rdap_ip_app.topic('to_process_IP', allow_empty=True)
topic_processed = rdap_ip_app.topic('collected_IP_data')

limiter_provider = LimiterProvider(component_config)


async def fetch_ip(address, client_v4: IPv4Client, client_v6: IPv6Client) \
        -> tuple[IPv4Response | IPv6Response | None, int | None, str | None]:
    try:
        ip = ipaddress.ip_address(address)
        # noinspection PyProtectedMember
        rdap_target = client_v4._get_rdap_server(ip) if ip.version == 4 else client_v6._get_rdap_server(ip)

        limiter = limiter_provider.get_limiter(rdap_target)
        limiter_result = await limiter.acquire()

        if limiter_result == limiter.IMMEDIATE_FAIL:
            return None, rc.LOCAL_RATE_LIMIT, f"No capacity left in the local rate limiter for {rdap_target}"
        elif limiter_result == limiter.TIMEOUT:
            return None, rc.LRL_TIMEOUT, f"Could not enter the limiter in {limiter.max_time} s for {rdap_target}"
        else:
            if ip.version == 4:
                ip_data = await client_v4.aio_lookup(address)
            else:
                ip_data = await client_v6.aio_lookup(address)
            return ip_data, 0, None
    except MalformedQueryError as e:
        return None, rc.INTERNAL_ERROR, str(e)
    except NotFoundError:
        return None, rc.NOT_FOUND, None
    except RateLimitError as e:
        return None, rc.RATE_LIMITED, str(e)
    except WhodapError as e:
        return None, rc.CANNOT_FETCH, str(e)
    except ValueError as e:
        return None, rc.INVALID_ADDRESS, str(e)
    except Exception as e:
        return None, rc.INTERNAL_ERROR, str(e)


async def process_entry(dn_ip, ipv4_client, ipv6_client):
    rdap_data, err_code, err_msg = await fetch_ip(dn_ip.ip, ipv4_client, ipv6_client)
    if rdap_data is not None:
        rdap_data = rdap_data.to_dict()

    result = RDAPIPResult(status_code=err_code, error=err_msg,
                          collector=COLLECTOR,
                          data=rdap_data)

    await topic_processed.send(key=dn_ip, value=result)


# The RDAP-IP processor
@rdap_ip_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    # The RDAP & WHOIS clients
    httpx_client = httpx.AsyncClient(verify=make_rdap_ssl_context(), follow_redirects=True,
                                     timeout=HTTP_TIMEOUT)

    while True:
        try:
            ipv4_client = await whodap.IPv4Client.new_aio_client(httpx_client=httpx_client)
            ipv6_client = await whodap.IPv6Client.new_aio_client(httpx_client=httpx_client)
            break
        except Exception as e:
            rdap_ip_app.logger.error("Error initializing RDAP clients. Retrying in 10 seconds.", exc_info=e)
            await asyncio.sleep(10)

    # Main message processing loop
    # dn is the domain name / IP address pair
    async for dn_ip, process_request in stream.items():
        dn_ip = ensure_model(IPToProcess, dn_ip)
        process_request = ensure_model(IPProcessRequest, process_request)

        try:
            # Omit the DN if the collector is not in the list of collectors to process
            if should_omit_ip(process_request, COLLECTOR):
                continue

            await process_entry(dn_ip, ipv4_client, ipv6_client)
        except Exception as e:
            ip = get_ip_safe(dn_ip)
            log_unhandled_error(e, COLLECTOR, ip, dn_ip=dn_ip)
            await handle_top_level_component_exception(e, COLLECTOR, dn_ip,
                                                       RDAPIPResult, topic_processed)

    await ipv4_client.aio_close()
    await ipv6_client.aio_close()
    await httpx_client.aclose()
