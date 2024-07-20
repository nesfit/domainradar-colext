import asyncio
import ipaddress

import httpx
import whodap
from whodap import IPv4Client, IPv6Client
from whodap.errors import *
from whodap.response import IPv4Response, IPv6Response

import common.result_codes as rc
from collectors.limiter import LimiterProvider
from collectors.util import make_rdap_ssl_context, should_omit_ip, handle_top_level_component_exception
from common import read_config, make_app, log
from common.models import IPToProcess, IPProcessRequest, RDAPIPResult
from common.util import ensure_model

COLLECTOR = "rdap-ip"
COMPONENT_NAME = "collector-" + COLLECTOR

# Read the config
config = read_config()
component_config = config.get(COLLECTOR, {})
logger = log.init(COMPONENT_NAME, config)

HTTP_TIMEOUT = component_config.get("http_timeout", 5)
CONCURRENCY = component_config.get("concurrency", 4)

# The Faust application
rdap_ip_app = make_app(COLLECTOR, config)

# The input and output topics
topic_to_process = rdap_ip_app.topic('to_process_IP', allow_empty=True)
topic_processed = rdap_ip_app.topic('collected_IP_data')

limiter_provider = LimiterProvider(component_config)


async def fetch_ip(dn, address, client_v4: IPv4Client, client_v6: IPv6Client) \
        -> tuple[IPv4Response | IPv6Response | None, int | None, str | None]:
    rdap_target = None
    limiter = None
    try:
        ip = ipaddress.ip_address(address)
        # noinspection PyProtectedMember
        rdap_target = client_v4._get_rdap_server(ip) if ip.version == 4 else client_v6._get_rdap_server(ip)

        limiter = limiter_provider.get_limiter(rdap_target)
        logger.k_trace("Acquiring limiter: %s / %s", dn, address, rdap_target)
        limiter_result = await limiter.acquire()

        if limiter_result == limiter.IMMEDIATE_FAIL:
            logger.k_debug("Local limiter immediate fail: %s / %s", dn, address, rdap_target)
            return None, rc.LOCAL_RATE_LIMIT, f"No capacity left in the local rate limiter for {rdap_target}"
        elif limiter_result == limiter.TIMEOUT:
            logger.k_debug("Local limiter timeout: %s / %s", dn, address, rdap_target)
            return None, rc.LRL_TIMEOUT, f"Could not enter the limiter in {limiter.max_time} s for {rdap_target}"

        if ip.version == 4:
            logger.k_trace("Fetching for IPv4: %s / %s", dn, address, rdap_target)
            ip_data = await client_v4.aio_lookup(address)
        else:
            logger.k_trace("Fetching for IPv6: %s / %s", dn, address, rdap_target)
            ip_data = await client_v6.aio_lookup(address)
        return ip_data, 0, None
    except httpx.TimeoutException:
        logger.k_debug("HTTP timeout", dn)
        return None, rc.TIMEOUT, None
    except (httpx.NetworkError, IOError) as e:
        logger.k_warning("Network error", dn, e=e)
        return None, rc.CANNOT_FETCH, "Network error: " + str(e)
    except BadStatusCode as e:
        logger.k_debug("RDAP weird status code - %s", str(e))
        return None, rc.CANNOT_FETCH, str(e)
    except MalformedQueryError as e:
        logger.k_warning("Malformed query: %s", dn, address, e=e)
        return None, rc.INTERNAL_ERROR, str(e)
    except NotFoundError:
        logger.k_debug("Not found: %s", dn, address)
        return None, rc.NOT_FOUND, None
    except RateLimitError as e:
        if limiter:
            limiter.fill_on_next()
        logger.k_info("Remote rate limited: %s / %s: %s", dn, address, rdap_target, str(e))
        return None, rc.RATE_LIMITED, str(e)
    except WhodapError as e:
        logger.k_warning("General whodap error: %s", dn, address, e=e)
        return None, rc.CANNOT_FETCH, str(e)
    except ValueError as e:
        logger.k_debug("Invalid address: %s", dn, address)
        return None, rc.INVALID_ADDRESS, str(e)
    except Exception as e:
        logger.k_warning("Unhandled exception: %s", dn, address, e=e)
        return None, rc.INTERNAL_ERROR, str(e)


async def process_entry(dn_ip, ipv4_client, ipv6_client):
    rdap_data, err_code, err_msg = await fetch_ip(dn_ip.domain_name, dn_ip.ip, ipv4_client, ipv6_client)
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
            logger.error("Error initializing RDAP clients. Retrying in 10 seconds.", exc_info=e)
            await asyncio.sleep(10)

    # Main message processing loop
    # dn is the domain name / IP address pair
    async for dn_ip, process_request in stream.items():
        dn_ip = ensure_model(IPToProcess, dn_ip)
        process_request = ensure_model(IPProcessRequest, process_request)

        if dn_ip is None:
            continue

        try:
            # Omit the DN if the collector is not in the list of collectors to process
            if should_omit_ip(process_request, COLLECTOR):
                logger.k_trace("Omitting IP %s", dn_ip.domain_name, dn_ip.ip)
                continue

            logger.k_trace("Processing %s", dn_ip.domain_name, dn_ip.ip)
            await process_entry(dn_ip, ipv4_client, ipv6_client)
        except Exception as e:
            logger.k_unhandled_error(e, str(dn_ip))
            await handle_top_level_component_exception(e, COMPONENT_NAME, dn_ip,
                                                       RDAPIPResult, topic_processed)

    await ipv4_client.aio_close()
    await ipv6_client.aio_close()
    await httpx_client.aclose()
