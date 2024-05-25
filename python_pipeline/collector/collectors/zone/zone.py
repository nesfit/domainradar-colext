import dns.exception
from dns.resolver import Cache
from faust.serializers import codecs

import common.result_codes as rc
from collectors.util import timestamp_now_millis, \
    handle_top_level_component_exception
from collectors.zone.collector import DNSCollectorOptions, DNSCollector
from common import read_config, make_app, StringCodec
from common.audit import log_unhandled_error
from common.models import RDAPRequest, RDAPDomainResult, ZoneRequest, ZoneResult, DNSRequest

codecs.register("str", StringCodec())

COLLECTOR = "zone"

# Read the config
config = read_config()
component_config = config.get(COLLECTOR, {})

DNS_SERVERS = component_config.get("dns_servers", ['195.113.144.194', '193.17.47.1',
                                                   '195.113.144.233', '185.43.135.1'])
TIMEOUT = component_config.get("timeout", 5)
ROTATE_NAMESERVERS = component_config.get("rotate_nameservers", False)
CONCURRENCY = component_config.get("concurrency", 4)

# The Faust application
zone_app = make_app(COLLECTOR, config)

# The input and output topics
topic_to_process = zone_app.topic('to_process_zone', key_type=str, value_type=ZoneRequest,
                                  key_serializer="str", allow_empty=True)

topic_processed_zone = zone_app.topic('processed_zone', key_type=str, value_type=ZoneResult,
                                      key_serializer="str")

topic_dns_requests = zone_app.topic('to_process_DNS', key_type=str, value_type=DNSRequest,
                                    key_serializer="str")

topic_rdap_requests = zone_app.topic('to_process_RDAP_DN', key_type=str, value_type=RDAPRequest,
                                     key_serializer="str")


# The Zone processor
@zone_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    options = DNSCollectorOptions(dns_servers=DNS_SERVERS, timeout=TIMEOUT, rotate_nameservers=ROTATE_NAMESERVERS)
    cache = Cache()
    collector = DNSCollector(options, zone_app.logger, cache)

    # Main message processing loop
    # dn is the domain name, req is the optional ZoneRequest object
    async for dn, req in stream.items():
        try:
            zone_app.logger.info("%s: Processing zone", dn)
            try:
                zone_info = await collector.get_zone_info(dn)
            except dns.exception.Timeout:
                zone_app.logger.info("%s: Timeout", dn)
                result = ZoneResult(status_code=rc.CANNOT_FETCH, error="Timeout",
                                    last_attempt=timestamp_now_millis(), zone=None)
            else:
                if zone_info is None:
                    zone_app.logger.info("%s: Zone not found", dn)
                    result = ZoneResult(status_code=rc.NOT_FOUND, error="Zone not found",
                                        last_attempt=timestamp_now_millis(), zone=None)
                else:
                    zone_app.logger.info("%s: Zone found: %s", dn, zone_info.zone)
                    result = ZoneResult(status_code=0, zone=zone_info,
                                        last_attempt=timestamp_now_millis())

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
            log_unhandled_error(e, COLLECTOR, dn)
            await handle_top_level_component_exception(e, COLLECTOR, dn, RDAPDomainResult, topic_processed_zone)
