import json
import logging
from typing import Any

import dns
from pydantic import BaseModel

from common.models import ZoneRequest, ZoneResult, DNSRequest, RDAPDomainRequest
import common.result_codes as rc
from common import ensure_model
from dns.resolver import Cache

from resolver import ZoneResolver
from dns_options import DNSCollectorOptions

logger = logging.getLogger()
cache = Cache()
options = DNSCollectorOptions.from_config({})
resolver = ZoneResolver(options, logger, cache)


def _dumps(obj: Any) -> bytes:
    if isinstance(obj, BaseModel):
        return (obj.model_dump_json(indent=None, by_alias=True) #, context={"separators": (',', ':')})
                .encode('utf-8', errors='backslashreplace'))
    elif isinstance(obj, str):
        return obj.encode('utf-8', errors='backslashreplace')
    else:
        return json.dumps(obj).encode('utf-8', errors='backslashreplace')


async def collect(input_consumer_records: list) -> dict[str, list[tuple[bytes, bytes]]]:
    ret_records = []
    ret_dns_requests = []
    ret_rdap_requests = []

    print("hello")

    for entry in input_consumer_records:
        try:
            dn = bytes(entry.key()).decode()
            value = entry.value()

            if value is None:
                req = None
            else:
                value = bytes(value)
                req = ensure_model(ZoneRequest, json.loads(value)) if len(value) > 0 else None

            if dn.endswith(".arpa"):
                result = ZoneResult(status_code=rc.INVALID_DOMAIN_NAME,
                                    error=".arpa domain names not supported",
                                    zone=None)
            else:
                try:
                    zone_info = await resolver.get_zone_info(dn)
                except dns.exception.Timeout:
                    logger.debug("Timeout", dn)
                    result = ZoneResult(status_code=rc.TIMEOUT, error=f"Timeout ({options.timeout} s)", zone=None)
                except dns.resolver.NoNameservers as e:
                    logger.debug("No nameservers", dn)
                    result = ZoneResult(status_code=rc.CANNOT_FETCH, error="SERVFAIL: " + str(e), zone=None)
                else:
                    if zone_info is None:
                        logger.debug("Zone not found", dn)
                        result = ZoneResult(status_code=rc.NOT_FOUND, error="Zone not found", zone=None)
                    else:
                        logger.debug("Zone found", dn, zone_info.zone)
                        result = ZoneResult(status_code=0, zone=zone_info)

            ret_records.append((_dumps(dn), _dumps(result)))

            if result.status_code == 0:
                if not req or req.collect_dns:
                    to_collect = req.dns_types_to_collect if req else None
                    to_process = req.dns_types_to_process_IPs_from if req else None
                    dns_req = DNSRequest(dns_types_to_collect=to_collect,
                                         dns_types_to_process_IPs_from=to_process,
                                         zone_info=result.zone)
                    ret_dns_requests.append((_dumps(dn), _dumps(dns_req)))

                if not req or req.collect_RDAP:
                    rdap_req = RDAPDomainRequest(zone=result.zone.zone)
                    ret_rdap_requests.append((_dumps(dn), _dumps(rdap_req)))

        except Exception as e:
            # TODO
            print(e)
            pass

    return {
        'processed_zone': ret_records,
        'to_process_DNS': ret_dns_requests,
        'to_process_RDAP_DN': ret_rdap_requests,
    }
