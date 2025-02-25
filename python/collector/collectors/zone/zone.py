"""zone.py: The processor for the zone collector."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import dns.exception
from dns.resolver import Cache

import common.result_codes as rc
from collectors.dns_options import DNSCollectorOptions
from collectors.util import make_top_level_exception_result
from collectors.zone.resolver import ZoneResolver
from common import log
from common.models import RDAPDomainRequest, ZoneRequest, ZoneResult, DNSRequest
from common.util import ensure_model, dump_model
from domrad_kafka_client import AsyncKafkaMessageProcessor, SimpleMessage

COLLECTOR = "zone"
COMPONENT_NAME = "collector-" + COLLECTOR


class ZoneProcessor(AsyncKafkaMessageProcessor):

    def __init__(self, config: dict):
        super().__init__(config)
        self._logger = log.init("worker")

        component_config = config.get(COLLECTOR, {})
        self._dns_options = DNSCollectorOptions.from_config(component_config)

        cache = Cache()
        self._collector = ZoneResolver(self._dns_options, self._logger, cache)

    async def process(self, key: bytes, value: bytes, partition: int, offset: int) -> list[SimpleMessage]:
        logger = self._logger
        dn = None
        ret = []

        try:
            dn = key.decode("utf-8")
            logger.k_trace("Processing zone", dn)
            req = ensure_model(ZoneRequest, value)

            if dn.endswith(".arpa"):
                result = ZoneResult(status_code=rc.INVALID_DOMAIN_NAME,
                                    error=".arpa domain names not supported",
                                    zone=None)
            else:
                try:
                    zone_info = await self._collector.get_zone_info(dn)
                except dns.exception.Timeout:
                    logger.k_debug("Timeout", dn)
                    result = ZoneResult(status_code=rc.TIMEOUT,
                                        error=f"Timeout ({self._dns_options.timeout} s)", zone=None)
                except dns.resolver.NoNameservers as e:
                    logger.k_debug("No nameservers", dn)
                    result = ZoneResult(status_code=rc.CANNOT_FETCH, error="SERVFAIL: " + str(e), zone=None)
                else:
                    if zone_info is None:
                        logger.k_debug("Zone not found", dn)
                        result = ZoneResult(status_code=rc.NOT_FOUND, error="Zone not found", zone=None)
                    else:
                        logger.k_trace("Zone found: %s", dn, zone_info.zone)
                        result = ZoneResult(status_code=0, zone=zone_info)

            ret.append(('processed_zone', key, dump_model(result)))

            if result.status_code == 0:
                if not req or req.collect_dns:
                    to_collect = req.dns_types_to_collect if req else None
                    to_process = req.dns_types_to_process_IPs_from if req else None
                    dns_req = DNSRequest(dns_types_to_collect=to_collect,
                                         dns_types_to_process_IPs_from=to_process,
                                         zone_info=result.zone)
                    ret.append(('to_process_DNS', key, dump_model(dns_req)))

                if not req or req.collect_RDAP:
                    rdap_req = RDAPDomainRequest(zone=result.zone.zone)
                    ret.append(('to_process_RDAP_DN', key, dump_model(rdap_req)))
        except Exception as e:
            logger.k_unhandled_error(e, dn)
            ret.append(make_top_level_exception_result('processed_zone', e, COMPONENT_NAME, dn, ZoneResult))

        return ret
