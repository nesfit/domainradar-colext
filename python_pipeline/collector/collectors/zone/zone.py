import dns.exception
from dns.resolver import Cache

import common.result_codes as rc
from collectors.options import DNSCollectorOptions
from collectors.util import handle_top_level_component_exception
from collectors.zone.collector import ZoneCollector
from common import read_config, make_app, log
from common.models import RDAPRequest, RDAPDomainResult, ZoneRequest, ZoneResult, DNSRequest
from common.util import ensure_model

COLLECTOR = "zone"
logger = log.get(COLLECTOR)

# Read the config
config = read_config()
component_config = config.get(COLLECTOR, {})

DNS_SERVERS = component_config.get("dns_servers", ['195.113.144.194', '193.17.47.1',
                                                   '195.113.144.233', '185.43.135.1'])
TIMEOUT = component_config.get("timeout", 5)
ROTATE_NAMESERVERS = component_config.get("rotate_nameservers", False)
CONCURRENCY = component_config.get("concurrency", 16)

# The Faust application
zone_app = make_app(COLLECTOR, config)

# The input and output topics
topic_to_process = zone_app.topic('to_process_zone', key_type=str, key_serializer='str', allow_empty=True)

topic_processed_zone = zone_app.topic('processed_zone', key_type=str, key_serializer='str')

topic_dns_requests = zone_app.topic('to_process_DNS', key_type=str, key_serializer='str')

topic_rdap_requests = zone_app.topic('to_process_RDAP_DN', key_type=str, key_serializer='str')


# The Zone processor
@zone_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    options = DNSCollectorOptions(dns_servers=DNS_SERVERS, timeout=TIMEOUT, rotate_nameservers=ROTATE_NAMESERVERS)
    cache = Cache()
    collector = ZoneCollector(options, logger, cache)

    # Main message processing loop
    # dn is the domain name, req is the optional ZoneRequest object
    async for dn, req in stream.items():
        try:
            logger.k_trace("Processing zone", dn)
            req = ensure_model(ZoneRequest, req)

            try:
                zone_info = await collector.get_zone_info(dn)
            except dns.exception.Timeout:
                logger.k_info("Timeout", dn)
                result = ZoneResult(status_code=rc.TIMEOUT, error=f"Timeout ({TIMEOUT} s)", zone=None)
            else:
                if zone_info is None:
                    logger.k_debug("Zone not found", dn)
                    result = ZoneResult(status_code=rc.NOT_FOUND, error="Zone not found", zone=None)
                else:
                    logger.k_debug("Zone found: %s", dn, zone_info.zone)
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
                    rdap_req = RDAPRequest(zone=result.zone.zone)
                    await topic_rdap_requests.send(key=dn, value=rdap_req)

        except Exception as e:
            logger.k_unhandled_error(e, dn)
            await handle_top_level_component_exception(e, COLLECTOR, dn, RDAPDomainResult, topic_processed_zone)
