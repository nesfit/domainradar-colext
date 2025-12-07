"""zone.py: The processor for the zone collector."""

__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import dns.exception
from dns.resolver import LRUCache

import common.result_codes as rc
from thor_collectors.dns_options import DNSCollectorOptions
from thor_collectors.processor import BaseAsyncCollectorProcessor
from thor_collectors.zone.resolver import ZoneResolver
from common import log
from common.models import RDAPDomainRequest, ZoneRequest, ZoneResult, DNSRequest
from common.util import dump_model
from kafka_multiprocessor import SimpleMessage, Message

COLLECTOR = "zone"
COMPONENT_NAME = "collector-" + COLLECTOR


class ZoneProcessor(BaseAsyncCollectorProcessor[str, ZoneRequest]):
    def __init__(self, config: dict):
        super().__init__(config, COMPONENT_NAME, "processed_zone", str, ZoneRequest, ZoneResult)
        self._logger = log.init("worker")

        component_config = config.get(COLLECTOR, {})
        self._dns_options = DNSCollectorOptions.from_config(component_config)

        cache_size = config.get("dns_cache_size")
        cache = LRUCache(cache_size) if cache_size else None
        self._collector = ZoneResolver(self._dns_options, self._logger, cache)

    async def process(self, message: Message[str, ZoneRequest]) -> list[SimpleMessage]:
        logger = self._logger
        dn = message.key
        req = message.value
        label = message.value.label if req else None
        ret = []

        logger.k_trace("Processing zone", dn)
        if dn.endswith(".arpa"):
            result = ZoneResult(
                status_code=rc.INVALID_DOMAIN_NAME,
                error=".arpa domain names not supported",
                zone=None,
                label=label,
            )
        else:
            try:
                zone_info = await self._collector.get_zone_info(dn)
            except dns.exception.Timeout:
                logger.k_debug("Timeout", dn)
                result = ZoneResult(
                    status_code=rc.TIMEOUT,
                    error=f"Timeout ({self._dns_options.timeout} s)",
                    zone=None,
                    label=label,
                )
            except dns.resolver.NoNameservers as e:
                logger.k_debug("No nameservers", dn)
                result = ZoneResult(
                    status_code=rc.CANNOT_FETCH,
                    error="SERVFAIL: " + str(e),
                    zone=None,
                    label=label,
                )
            else:
                if zone_info is None:
                    logger.k_debug("Zone not found", dn)
                    result = ZoneResult(
                        status_code=rc.NOT_FOUND, error="Zone not found", zone=None, label=label
                    )
                else:
                    logger.k_trace("Zone found: %s", dn, zone_info.zone)
                    result = ZoneResult(status_code=0, zone=zone_info, label=label)

        ret.append(("processed_zone", message.key_raw, dump_model(result)))

        if result.status_code == 0:
            if not req or req.collect_dns:
                to_collect = req.dns_types_to_collect if req else None
                to_process = req.dns_types_to_process_IPs_from if req else None
                dns_req = DNSRequest(
                    dns_types_to_collect=to_collect,
                    dns_types_to_process_IPs_from=to_process,
                    zone_info=result.zone,
                )
                ret.append(("to_process_DNS", message.key_raw, dump_model(dns_req)))

            if not req or req.collect_RDAP:
                rdap_req = RDAPDomainRequest(zone=result.zone.zone)
                ret.append(("to_process_RDAP_DN", message.key_raw, dump_model(rdap_req)))

        return ret
