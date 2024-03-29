import ipaddress

import faust

from asyncwhois.errors import *
import whodap
from whodap import IPv4Client, IPv6Client
from whodap.response import IPv4Response, IPv6Response
from whodap.errors import *
import httpx

import result_codes
from drcol.custom_codecs import StringCodec
from drcol.util import make_ssl_context, timestamp_now_millis
from drcol.models import *


async def fetch_ip(address, client_v4: IPv4Client, client_v6: IPv6Client) \
        -> tuple[IPv4Response | IPv6Response | None, int | None, str | None]:
    try:
        ip = ipaddress.ip_address(address)
        if ip.version == 4:
            ip_data = await client_v4.aio_lookup(address)
        else:
            ip_data = await client_v6.aio_lookup(address)

        return ip_data, 0, None
    except MalformedQueryError as e:
        return None, result_codes.INTERNAL_ERROR, str(e)
    except NotFoundError:
        return None, result_codes.NOT_FOUND, "RDAP entity not found"
    except RateLimitError as e:
        return None, result_codes.RATE_LIMITED, str(e)
    except WhodapError as e:
        return None, result_codes.OTHER_EXTERNAL_ERROR, str(e)
    except Exception as e:
        return None, result_codes.INTERNAL_ERROR, str(e)


# The Faust application
rdap_ip_app = faust.App('drcol-rdap-ip',
                        broker='kafka://localhost:9092',
                        debug=True)

# The input and output topics
topic_to_process = rdap_ip_app.topic('to_process_IP', key_type=IPToProcess, value_type=None, allow_empty=True)

topic_processed = rdap_ip_app.topic('collected_IP_data', key_type=IPToProcess, value_type=RDAPDomainResult)


# The RDAP-DN processor
@rdap_ip_app.agent(topic_to_process, concurrency=4)
async def process_entries(stream):
    # The RDAP & WHOIS clients
    httpx_client = httpx.AsyncClient(verify=make_ssl_context(), follow_redirects=True, timeout=5)
    ipv4_client = await whodap.IPv4Client.new_aio_client(httpx_client=httpx_client)
    ipv6_client = await whodap.IPv6Client.new_aio_client(httpx_client=httpx_client)

    # Main message processing loop
    # dn is the domain name / IP address pair
    async for dn_ip, _ in stream.items():
        # TODO: implement a per-endpoint local rate limiter (see aiolimiter)
        rdap_data, err_code, err_msg = await fetch_ip(dn_ip.ip, ipv4_client, ipv6_client)
        if rdap_data is not None:
            rdap_data = rdap_data.to_dict()

        result = RDAPIPResult(status_code=err_code, error=err_msg,
                              last_attempt=timestamp_now_millis(),
                              collector="rdap-ip",
                              data=rdap_data)

        # (this could probably be send_soon not to block the loop)
        await topic_processed.send(key=dn_ip, value=result)

    await ipv4_client.aio_close()
    await ipv6_client.aio_close()
    await httpx_client.aclose()


if __name__ == '__main__':
    rdap_ip_app.main()
