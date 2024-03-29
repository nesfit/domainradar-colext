from types import NoneType
from typing import Any

import faust
import whodap
from whodap import DNSClient
from whodap.response import DomainResponse
import whodap.errors
import asyncwhois

from drcol.util import extract_known_tld
from util import fetch_entities
from models import *
from datetime import datetime
from faust.serializers import codecs
import result_codes


class StrCodec(codecs.Codec):

    def _dumps(self, obj: Any) -> bytes:
        return str(obj).encode("utf-8")

    def _loads(self, s: bytes) -> Any:
        return s.decode("utf-8")


codecs.register("str", StrCodec())


async def fetch_rdap(domain_name, client: DNSClient) \
        -> tuple[DomainResponse | None, list | None, int | None, str | None]:
    domain, tld = extract_known_tld(domain_name, client)
    if domain is None or tld is None:
        return None, None, result_codes.RDAP_NOT_AVAILABLE, "No RDAP endpoint available for the domain name"

    try:
        rdap_data = await client.aio_lookup(domain, tld)
        entities = await fetch_entities(rdap_data, client)
        return rdap_data, entities, 0, None
    except whodap.errors.MalformedQueryError:
        return None, None, result_codes.INTERNAL_ERROR, "RDAP reported malformed query"
    except whodap.errors.NotFoundError:
        return None, None, result_codes.NOT_FOUND, "RDAP data not found"
    except whodap.errors.RateLimitError:
        return None, None, result_codes.RATE_LIMITED, "Too many requests"
    except whodap.errors.WhodapError as e:
        return None, None, result_codes.OTHER_EXTERNAL_ERROR, str(e)
    except Exception as e:
        return None, None, result_codes.INTERNAL_ERROR, str(e)


# The Faust application
app = faust.App('drcol-rdap-dn',
                broker='kafka://localhost:9092',
                debug=True)

# The input and output topics
topic_to_process = app.topic('to_process_RDAP_DN', key_type=str, value_type=RDAPRequest,
                             key_serializer="str", allow_empty=True)

topic_processed = app.topic('processed_RDAP_DN', key_type=str, value_type=RDAPDomainResult,
                            key_serializer="str")


# The RDAP-DN processor
@app.agent(topic_to_process, concurrency=1)
async def process_entries(stream):
    # The RDAP & WHOIS clients
    rdap_client = await whodap.DNSClient.new_aio_client()
    whois_client = asyncwhois.client.DomainClient()

    async for dn, req in stream.items():
        whois_raw, whois_parsed = None, None
        zone = None

        # Extract the zone DN if present
        if req is not None and req.zone is not None:
            zone = req.zone

        for_source = True
        rdap_data, entities, err_code, err_msg = await fetch_rdap(dn, rdap_client)
        if rdap_data is None and zone is not None and dn != zone:
            # Try the zone name
            for_source = False
            rdap_data, entities, err_code, err_msg = await fetch_rdap(zone, rdap_client)

        if rdap_data is None:
            try:
                whois_raw, whois_parsed = await whois_client.aio_whois(dn if zone is None else zone)

                if whois_parsed is not None:
                    whois_parsed = {k: v for k, v in whois_parsed.items() if v is not None
                                    and (not isinstance(v, list) or len(v) > 0)}

            except Exception as e:
                # TODO
                print(f'Error fetching WHOIS for {dn}: {e}')
                continue
        else:
            rdap_data = rdap_data.to_dict()
            entities = [e.to_dict() for e in entities]

        result = RDAPDomainResult(statusCode=err_code, error=err_msg,
                                  last_attempt=int(datetime.utcnow().timestamp() * 1e3),
                                  rdap_data=rdap_data, entities=entities, raw_whois_data=whois_raw,
                                  parsed_whois_data=whois_parsed, is_for_source_name=for_source)

        await topic_processed.send(key=dn, value=result)

    await rdap_client.aio_close()


if __name__ == '__main__':
    app.main()
