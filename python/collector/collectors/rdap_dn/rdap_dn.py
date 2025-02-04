"""rdap_dn.py: The Faust application for the RDAP-DN collector."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import asyncio

import asyncwhois
import httpx
import tldextract
import whodap
from asyncwhois import DomainClient
from asyncwhois.errors import *
from asyncwhois.query import DomainQuery
from whodap import DNSClient
from whodap.errors import *
from whodap.response import DomainResponse

import common.result_codes as rc
from collectors.limiter import LimiterProvider
from collectors.rdap_util import fetch_entities, make_rdap_ssl_context, extract_known_tld
from collectors.util import handle_top_level_exception
from common import read_config, make_app, log
from common.models import RDAPDomainRequest, RDAPDomainResult
from common.util import ensure_model

COLLECTOR = "rdap-dn"
COMPONENT_NAME = "collector-" + COLLECTOR

# Read the config
config = read_config()
component_config = config.get(COLLECTOR, {})
logger = log.init(COMPONENT_NAME)

HTTP_TIMEOUT = component_config.get("http_timeout", 5)
CONCURRENCY = component_config.get("concurrency", 4)

# The Faust application
rdap_dn_app = make_app(COLLECTOR, config)

# The input and output topics
topic_to_process = rdap_dn_app.topic('to_process_RDAP_DN', key_type=str, key_serializer='str', allow_empty=True)
topic_processed = rdap_dn_app.topic('processed_RDAP_DN', key_type=str, key_serializer='str')

limiter_provider = LimiterProvider(component_config)


async def fetch_rdap(domain_name, client: DNSClient) \
        -> tuple[DomainResponse | None, list | None, int | None, str | None]:
    domain, tld, rdap_base = extract_known_tld(domain_name, client)
    if domain is None or tld is None:
        return None, None, rc.NO_ENDPOINT, None

    limiter = limiter_provider.get_limiter(rdap_base, tld)
    limiter_result = await limiter.acquire()

    if limiter_result == limiter.IMMEDIATE_FAIL:
        logger.k_debug("Local limiter immediate fail: %s", domain_name, rdap_base)
        return None, None, rc.LOCAL_RATE_LIMIT, f"No capacity left in the limiter for {rdap_base}"
    elif limiter_result == limiter.TIMEOUT:
        logger.k_debug("Local limiter timeout: %s", domain_name, rdap_base)
        return None, None, rc.LRL_TIMEOUT, f"Could not enter the limiter in {limiter.max_time} s for {rdap_base}"

    try:
        logger.k_trace("Fetching RDAP: %s", domain_name, rdap_base)
        rdap_data = await client.aio_lookup(domain, tld)
        entities = await fetch_entities(rdap_data, client)
        return rdap_data, entities, 0, None
    except httpx.TimeoutException:
        logger.k_debug("HTTP timeout", domain_name)
        return None, None, rc.TIMEOUT, None
    except (httpx.NetworkError, IOError) as e:
        logger.k_warning("Network error", domain_name, e=e)
        return None, None, rc.CANNOT_FETCH, "Network error: " + str(e)
    except BadStatusCode as e:
        logger.k_debug("RDAP weird status code - %s", str(e))
        return None, None, rc.CANNOT_FETCH, str(e)
    except MalformedQueryError as e:
        logger.k_warning("Malformed query", domain_name, e=e)
        return None, None, rc.INTERNAL_ERROR, str(e)
    except NotFoundError:
        logger.k_debug("Not found", domain_name)
        return None, None, rc.NOT_FOUND, None
    except RateLimitError as e:
        limiter.fill_on_next()
        logger.k_debug("Remote rate limited: %s: %s", domain_name, rdap_base, str(e))
        return None, None, rc.RATE_LIMITED, str(e)
    except WhodapError as e:
        logger.k_warning("General whodap error", domain_name, e=e)
        return None, None, rc.CANNOT_FETCH, str(e)
    except Exception as e:
        logger.k_warning("Unhandled exception", domain_name, e=e)
        return None, None, rc.INTERNAL_ERROR, str(e)


async def fetch_whois(domain_name, client: DomainClient) -> tuple[str | None, dict | None, int | None, str | None]:
    suffix = tldextract.extract(domain_name).suffix
    suffix = suffix.split(".")[-1]
    # noinspection PyProtectedMember
    endpoint = DomainQuery._get_server_name(suffix) or DomainQuery.iana_server

    limiter = limiter_provider.get_limiter(endpoint, suffix)
    logger.k_trace("Acquiring limiter", domain_name)
    limiter_result = await limiter.acquire()

    if limiter_result == limiter.IMMEDIATE_FAIL:
        logger.k_debug("Local limiter immediate fail: %s", domain_name, endpoint)
        return None, None, rc.LOCAL_RATE_LIMIT, f"No capacity left in the limiter for {endpoint}"
    elif limiter_result == limiter.TIMEOUT:
        logger.k_debug("Local limiter timeout: %s", domain_name, endpoint)
        return None, None, rc.LRL_TIMEOUT, f"Could not enter the limiter in {limiter.max_time} s for {endpoint}"

    try:
        whois_raw, whois_parsed = await client.aio_whois(domain_name)

        if whois_parsed is not None:
            whois_parsed = {k: v for k, v in whois_parsed.items() if v is not None
                            and (not isinstance(v, list) or len(v) > 0)}

        return whois_raw, whois_parsed, 0, None
    except NotFoundError as e:
        logger.k_debug("WHOIS not found", domain_name)
        return None, None, rc.NOT_FOUND, str(e)
    except WhoIsError as e:
        msg = str(e).lower()
        if "rate" in msg or "limit" in msg:
            logger.k_info("WHOIS remote rate limited: %s: %s", domain_name, endpoint, msg)
            return None, None, rc.RATE_LIMITED, str(e)
        elif "domain not found" in msg:
            logger.k_debug("WHOIS not found", domain_name)
            return None, None, rc.NOT_FOUND, str(e)

        logger.k_debug("WHOIS other error: %s: %s", domain_name, endpoint, msg)
        return None, None, rc.CANNOT_FETCH, str(e)
    except ConnectionResetError as e:
        logger.k_debug("WHOIS connection reset error", domain_name)
        return None, None, rc.CANNOT_FETCH, str(e)
    except Exception as e:
        logger.k_warning("Unhandled WHOIS exception", domain_name, e=e)
        if isinstance(e, IOError):
            return None, None, rc.INTERNAL_ERROR, str(e)
        else:
            return None, None, rc.CANNOT_FETCH, str(e)


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
    logger.k_trace("Got result %s for target %s", dn, err_code, rdap_target)

    if rdap_data is None:
        # If the RDAP data is not available for the source DN, try to get it for the registered domain name
        # (i.e. one level above the public suffix)
        target_parts = tldextract.extract(rdap_target)
        if target_parts.domain != "" and target_parts.suffix != "":
            reg_rdap_target = target_parts.domain + "." + target_parts.suffix
            if reg_rdap_target != rdap_target:
                rdap_target = reg_rdap_target
                logger.k_trace("Retrying on target %s", dn, rdap_target)
                rdap_data, entities, err_code, err_msg = await fetch_rdap(rdap_target, rdap_client)

    if rdap_data is None:
        # Only try WHOIS if no RDAP data is available at any level of the source DN
        logger.k_debug("No RDAP, trying WHOIS for %s", dn, zone or dn)
        (whois_raw, whois_parsed,
         whois_err_code, whois_err_msg) = await fetch_whois(zone or dn, whois_client)
    else:
        logger.k_trace("Got RDAP data for %s", dn, rdap_target)
        # Convert the SimpleNamespace objects to JSON-serializable dictionaries
        rdap_data = rdap_data.to_dict()
        entities = [e.to_dict() for e in entities]

    result = RDAPDomainResult(status_code=err_code, error=err_msg,
                              rdap_data=rdap_data, entities=entities,
                              rdap_target=rdap_target,
                              whois_status_code=whois_err_code, whois_error=whois_err_msg,
                              raw_whois_data=whois_raw, parsed_whois_data=whois_parsed)

    logger.k_trace("Sending RDAP result", dn)
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
            logger.error("Error initializing RDAP client. Retrying in 10 seconds.", exc_info=e)
            await asyncio.sleep(10)

    # Main message processing loop
    # dn is the domain name, req is the optional RDAPRequest object
    async for dn, req in stream.items():
        logger.k_trace("Processing RDAP", dn)
        req = ensure_model(RDAPDomainRequest, req)

        try:
            await process_entry(dn, req, rdap_client, whois_client)
        except Exception as e:
            logger.k_unhandled_error(e, dn)
            await handle_top_level_exception(e, COMPONENT_NAME, dn, RDAPDomainResult, topic_processed)

    await rdap_client.aio_close()
    await httpx_client.aclose()
