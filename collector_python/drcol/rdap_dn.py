import faust
from faust.serializers import codecs

import asyncwhois
from asyncwhois import DomainClient
from asyncwhois.errors import *
import whodap
from whodap import DNSClient
from whodap.response import DomainResponse
from whodap.errors import *
import httpx

import result_codes
from drcol.custom_codecs import StringCodec
from drcol.util import fetch_entities, extract_known_tld, make_ssl_context, timestamp_now_millis
from drcol.models import *

codecs.register("str", StringCodec())


async def fetch_rdap(domain_name, client: DNSClient) \
        -> tuple[DomainResponse | None, list | None, int | None, str | None]:
    domain, tld = extract_known_tld(domain_name, client)
    if domain is None or tld is None:
        return None, None, result_codes.RDAP_NOT_AVAILABLE, "No RDAP endpoint available for the domain name"

    try:
        rdap_data = await client.aio_lookup(domain, tld)
        entities = await fetch_entities(rdap_data, client)
        return rdap_data, entities, 0, None
    except MalformedQueryError as e:
        return None, None, result_codes.INTERNAL_ERROR, str(e)
    except NotFoundError:
        return None, None, result_codes.NOT_FOUND, "RDAP entity not found"
    except RateLimitError as e:
        return None, None, result_codes.RATE_LIMITED, str(e)
    except WhodapError as e:
        return None, None, result_codes.OTHER_EXTERNAL_ERROR, str(e)
    except Exception as e:
        return None, None, result_codes.INTERNAL_ERROR, str(e)


async def fetch_whois(domain_name, client: DomainClient) -> tuple[str | None, dict | None, int | None, str | None]:
    try:
        whois_raw, whois_parsed = await client.aio_whois(domain_name)

        if whois_parsed is not None:
            whois_parsed = {k: v for k, v in whois_parsed.items() if v is not None
                            and (not isinstance(v, list) or len(v) > 0)}

        return whois_raw, whois_parsed, 0, None
    except NotFoundError as e:
        return None, None, result_codes.NOT_FOUND, str(e)
    except WhoIsError as e:
        msg = str(e).lower()
        if "rate" in msg or "limit" in msg:
            return None, None, result_codes.RATE_LIMITED, str(e)
        elif "domain not found" in msg:
            return None, None, result_codes.NOT_FOUND, str(e)
        return None, None, result_codes.OTHER_EXTERNAL_ERROR, str(e)


# The Faust application
rdap_dn_app = faust.App('drcol-rdap-dn',
                        broker='kafka://localhost:9092',
                        debug=True)

# The input and output topics
topic_to_process = rdap_dn_app.topic('to_process_RDAP_DN', key_type=str, value_type=RDAPRequest,
                                     key_serializer="str", allow_empty=True)

topic_processed = rdap_dn_app.topic('processed_RDAP_DN', key_type=str, value_type=RDAPDomainResult,
                                    key_serializer="str")


# The RDAP-DN processor
@rdap_dn_app.agent(topic_to_process, concurrency=4)
async def process_entries(stream):
    # The RDAP & WHOIS clients
    httpx_client = httpx.AsyncClient(verify=make_ssl_context(), follow_redirects=True, timeout=5)
    rdap_client = await whodap.DNSClient.new_aio_client(httpx_client=httpx_client)
    whois_client = asyncwhois.client.DomainClient()

    # Main message processing loop
    # dn is the domain name, req is the optional RDAPRequest object
    async for dn, req in stream.items():
        # TODO: implement a per-endpoint local rate limiter (see aiolimiter)
        # The default WHOIS results are empty and with a status code signalising that WHOIS was not used
        whois_raw, whois_parsed, whois_err_code, whois_err_msg = None, None, result_codes.WHOIS_NOT_PERFORMED, None
        zone = None

        # Extract the zone DN if present in the request
        if req is not None and req.zone is not None:
            zone = req.zone

        if zone is not None:
            # If the zone DN is available, get RDAP data for it. There's no point in trying the actual
            # source domain name, RDAP should only provide data for points of delegation
            for_source = zone == dn
            rdap_data, entities, err_code, err_msg = await fetch_rdap(zone, rdap_client)
        else:
            # If the zone DN is not available, try to get RDAP data for the actual source domain name
            for_source = True
            rdap_data, entities, err_code, err_msg = await fetch_rdap(dn, rdap_client)

        if rdap_data is None:
            # Only try WHOIS if RDAP data is not available
            (whois_raw, whois_parsed,
             whois_err_code, whois_err_msg) = await fetch_whois(zone or dn, whois_client)
        else:
            # Convert the SimpleNamespace objects to JSON-serializable dictionaries
            rdap_data = rdap_data.to_dict()
            entities = [e.to_dict() for e in entities]

        result = RDAPDomainResult(status_code=err_code, error=err_msg,
                                  last_attempt=timestamp_now_millis(),
                                  rdap_data=rdap_data, entities=entities,
                                  is_for_source_name=for_source,
                                  whois_status_code=whois_err_code, whois_error=whois_err_msg,
                                  raw_whois_data=whois_raw, parsed_whois_data=whois_parsed)

        # (this could probably be send_soon not to block the loop)
        await topic_processed.send(key=dn, value=result)

    await rdap_client.aio_close()
    await httpx_client.aclose()


if __name__ == '__main__':
    rdap_dn_app.main()
