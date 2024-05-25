import asyncio

from aiolimiter import AsyncLimiter
from faust.serializers import codecs
import httpx
import tldextract

import asyncwhois
from asyncwhois import DomainClient
from asyncwhois.errors import *

import whodap
from whodap import DNSClient
from whodap.response import DomainResponse
from whodap.errors import *

from common import read_config, make_app, StringCodec
from common.audit import log_unhandled_error
from common.models import RDAPRequest, RDAPDomainResult
import common.result_codes as rc
from collectors.util import fetch_entities, extract_known_tld, make_rdap_ssl_context, timestamp_now_millis, \
    handle_top_level_component_exception

codecs.register("str", StringCodec())

COLLECTOR = "rdap_dn"

# Read the config
config = read_config()
component_config = config.get(COLLECTOR, {})

LIMITER_CONCURRENCY = component_config.get("limiter_concurrency", 5)
LIMITER_WINDOW = component_config.get("limiter_window", 60)
HTTP_TIMEOUT = component_config.get("http_timeout", 5)
CONCURRENCY = component_config.get("concurrency", 4)

# The Faust application
rdap_dn_app = make_app(COLLECTOR, config)

# The input and output topics
topic_to_process = rdap_dn_app.topic('to_process_RDAP_DN', key_type=str, value_type=RDAPRequest,
                                     key_serializer="str", allow_empty=True)

topic_processed = rdap_dn_app.topic('processed_RDAP_DN', key_type=str, value_type=RDAPDomainResult,
                                    key_serializer="str")

_limiters: dict[str, AsyncLimiter] = {}


def get_limiter(endpoint: str) -> AsyncLimiter:
    if endpoint not in _limiters:
        _limiters[endpoint] = AsyncLimiter(LIMITER_CONCURRENCY, LIMITER_WINDOW)
    return _limiters[endpoint]


async def fetch_rdap(domain_name, client: DNSClient) \
        -> tuple[DomainResponse | None, list | None, int | None, str | None]:
    domain, tld, rdap_base = extract_known_tld(domain_name, client)
    if domain is None or tld is None:
        return None, None, rc.RDAP_NOT_AVAILABLE, "No RDAP endpoint available for the domain name"
    try:
        async with get_limiter(rdap_base):
            rdap_data = await client.aio_lookup(domain, tld)
            entities = await fetch_entities(rdap_data, client)
            return rdap_data, entities, 0, None
    except MalformedQueryError as e:
        return None, None, rc.INTERNAL_ERROR, str(e)
    except NotFoundError:
        return None, None, rc.NOT_FOUND, "RDAP entity not found"
    except RateLimitError as e:
        await get_limiter(rdap_base).acquire(LIMITER_CONCURRENCY)
        return None, None, rc.RATE_LIMITED, str(e)
    except WhodapError as e:
        return None, None, rc.OTHER_EXTERNAL_ERROR, str(e)
    except Exception as e:
        return None, None, rc.INTERNAL_ERROR, str(e)


async def fetch_whois(domain_name, client: DomainClient) -> tuple[str | None, dict | None, int | None, str | None]:
    try:
        whois_raw, whois_parsed = await client.aio_whois(domain_name)

        if whois_parsed is not None:
            whois_parsed = {k: v for k, v in whois_parsed.items() if v is not None
                            and (not isinstance(v, list) or len(v) > 0)}

        return whois_raw, whois_parsed, 0, None
    except NotFoundError as e:
        return None, None, rc.NOT_FOUND, str(e)
    except WhoIsError as e:
        msg = str(e).lower()
        if "rate" in msg or "limit" in msg:
            return None, None, rc.RATE_LIMITED, str(e)
        elif "domain not found" in msg:
            return None, None, rc.NOT_FOUND, str(e)
        return None, None, rc.OTHER_EXTERNAL_ERROR, str(e)


async def process_entry(dn, req, rdap_client, whois_client):
    # The default WHOIS results are empty and with a status code signalising that WHOIS was not used
    whois_raw, whois_parsed, whois_err_code, whois_err_msg = None, None, rc.WHOIS_NOT_PERFORMED, None
    zone = None

    # Extract the zone DN if present in the request
    if req is not None and req.zone is not None:
        zone = req.zone

    if zone is not None:
        # If the zone DN is available, get RDAP data for it. There's no point in trying the actual
        # source domain name, RDAP should only provide data for points of delegation
        rdap_target = zone
    else:
        # If the zone DN is not available, try to get RDAP data for the actual source domain name
        rdap_target = dn

    rdap_data, entities, err_code, err_msg = await fetch_rdap(rdap_target, rdap_client)

    if rdap_data is None:
        # If the RDAP data is not available for the source DN, try to get it for the registered domain name
        # (i.e. one level above the public suffix)
        target_parts = tldextract.extract(rdap_target)
        if target_parts.domain != "" and target_parts.suffix != "":
            rdap_target = target_parts.domain + "." + target_parts.suffix
            rdap_data, entities, err_code, err_msg = await fetch_rdap(rdap_target, rdap_client)

    if rdap_data is None:
        # Only try WHOIS if no RDAP data is available at any level of the source DN
        (whois_raw, whois_parsed,
         whois_err_code, whois_err_msg) = await fetch_whois(zone or dn, whois_client)
    else:
        # Convert the SimpleNamespace objects to JSON-serializable dictionaries
        rdap_data = rdap_data.to_dict()
        entities = [e.to_dict() for e in entities]

    result = RDAPDomainResult(status_code=err_code, error=err_msg,
                              last_attempt=timestamp_now_millis(),
                              rdap_data=rdap_data, entities=entities,
                              rdap_target=rdap_target,
                              whois_status_code=whois_err_code, whois_error=whois_err_msg,
                              raw_whois_data=whois_raw, parsed_whois_data=whois_parsed)

    # (this could probably be send_soon not to block the loop)
    await topic_processed.send(key=dn, value=result)


# The RDAP-DN processor
@rdap_dn_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    # The RDAP & WHOIS clients
    httpx_client = httpx.AsyncClient(verify=make_rdap_ssl_context(), follow_redirects=True,
                                     timeout=HTTP_TIMEOUT)
    whois_client = asyncwhois.client.DomainClient()

    while True:
        try:
            rdap_client = await whodap.DNSClient.new_aio_client(httpx_client=httpx_client)
            break
        except Exception as e:
            rdap_dn_app.logger.error("Error initializing RDAP client. Retrying in 10 seconds.", exc_info=e)
            await asyncio.sleep(10)

    # Main message processing loop
    # dn is the domain name, req is the optional RDAPRequest object
    async for dn, req in stream.items():
        try:
            await process_entry(dn, req, rdap_client, whois_client)
        except Exception as e:
            log_unhandled_error(e, COLLECTOR, dn)
            await handle_top_level_component_exception(e, COLLECTOR, dn, RDAPDomainResult, topic_processed)

    await rdap_client.aio_close()
    await httpx_client.aclose()
