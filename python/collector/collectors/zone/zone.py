"""zone.py: The Faust application for the zone collector."""
__authors__ = ["Ondřej Ondryáš <xondry02@vut.cz>", "Matěj Čech <xcechm15@stud.fit.vut.cz>"]

import dns.exception
from dns.resolver import Cache

import common.result_codes as rc
from collectors.dns_options import DNSCollectorOptions
from collectors.util import handle_top_level_exception
from collectors.zone.resolver import ZoneResolver
from common import read_config, make_app, log
from common.models import RDAPDomainRequest, RDAPDomainResult, ZoneRequest, ZoneResult, DNSRequest, DNToProcess
from common.util import ensure_model

COLLECTOR = "zone"
COMPONENT_NAME = "collector-" + COLLECTOR

# Read the config
config = read_config()
component_config = config.get(COLLECTOR, {})
logger = log.init(COMPONENT_NAME, config)

DNS_OPTIONS = DNSCollectorOptions.from_config(component_config)
CONCURRENCY = component_config.get("concurrency", 16)

# The Faust application
zone_app = make_app(COLLECTOR, config)

# The input and output topics
topic_to_process = zone_app.topic('to_process_zone', key_type=str, key_serializer='str', allow_empty=True)

topic_processed_zone = zone_app.topic('processed_zone', key_type=str, key_serializer='str')

topic_dns_requests = zone_app.topic('to_process_DNS', key_type=str, key_serializer='str')

topic_rdap_requests = zone_app.topic('to_process_RDAP_DN', key_type=str, key_serializer='str')

topic_dn_requests = zone_app.topic('to_process_DN', allow_empty=True, value_type=bytes, value_serializer='raw')


# The Zone processor
@zone_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    cache = Cache()
    collector = ZoneResolver(DNS_OPTIONS, logger, cache)

    # Main message processing loop
    # dn is the domain name, req is the optional ZoneRequest object
    async for dn, req in stream.items():
        try:
            logger.k_trace("Processing zone", dn)
            req = ensure_model(ZoneRequest, req)

            if dn.endswith(".arpa"):
                result = ZoneResult(status_code=rc.INVALID_DOMAIN_NAME,
                                    error=".arpa domain names not supported",
                                    zone=None)
            else:
                try:
                    zone_info = await collector.get_zone_info(dn)
                except dns.exception.Timeout:
                    logger.k_debug("Timeout", dn)
                    result = ZoneResult(status_code=rc.TIMEOUT, error=f"Timeout ({DNS_OPTIONS.timeout} s)", zone=None)
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

            await topic_processed_zone.send(key=dn, value=result)

            if result.status_code == 0:
                if not req or req.collect_dns:
                    to_collect = req.dns_types_to_collect if req else None
                    to_process = req.dns_types_to_process_IPs_from if req else None
                    dns_req = DNSRequest(dns_types_to_collect=to_collect,
                                         dns_types_to_process_IPs_from=to_process,
                                         zone_info=result.zone)
                    await topic_dns_requests.send(key=dn, value=dns_req)

                if not req or req.collect_RDAP:
                    rdap_req = RDAPDomainRequest(zone=result.zone.zone)
                    await topic_rdap_requests.send(key=dn, value=rdap_req)

                if not req or req.collect_dn_data:
                    dn_to_process = DNToProcess(domain_name=dn)
                    await topic_dn_requests.send(key=dn_to_process, value=None)

        except Exception as e:
            logger.k_unhandled_error(e, dn)
            await handle_top_level_exception(e, COMPONENT_NAME, dn, RDAPDomainResult, topic_processed_zone)
