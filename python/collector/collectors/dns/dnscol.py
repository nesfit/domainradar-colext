"""dnscol.py: The Faust application for the DNS collector."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from dns.resolver import Cache

from collectors.dns.scanner import DNSScanner
from collectors.dns_options import DNSCollectorOptions
from collectors.util import handle_top_level_exception
from common import read_config, make_app, log
from common.models import DNSRequest, DNSResult, IPToProcess, DNSData
from common.util import ensure_model

COLLECTOR = "dns"
COMPONENT_NAME = "collector-" + COLLECTOR

# Read the config
config = read_config()
component_config = config.get(COLLECTOR, {})
logger = log.init(COMPONENT_NAME, config)

DNS_OPTIONS = DNSCollectorOptions.from_config(component_config)
SCANNER_LOG_LEVEL = component_config.get("scanner_log_level", "INFO")
CONCURRENCY = component_config.get("concurrency", 16)

# The Faust application
dns_app = make_app(COLLECTOR, config)

# The input and output topics
topic_to_process = dns_app.topic('to_process_DNS', key_type=str, key_serializer='str')

topic_processed_dns = dns_app.topic('processed_DNS', key_type=str, key_serializer='str')

topic_tls_requests = dns_app.topic('to_process_TLS', key_type=str, key_serializer='str',
                                   value_type=str, value_serializer='str')

topic_ip_requests = dns_app.topic('to_process_IP', value_type=bytes, value_serializer='raw',
                                  allow_empty=True)


def get_ip_for_tls(dns_data: DNSData) -> str | None:
    if dns_data is None:
        return None

    if dns_data.cname is not None and dns_data.cname.related_ips is not None \
            and len(dns_data.cname.related_ips) > 0:
        return next(iter(dns_data.cname.related_ips))
    elif dns_data.a is not None and len(dns_data.a) > 0:
        return next(iter(dns_data.a))
    elif dns_data.aaaa is not None and len(dns_data.aaaa) > 0:
        return next(iter(dns_data.aaaa))

    return None


# The DNS processor
@dns_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    scanner_child_logger = logger.getChild("scanner")
    scanner_child_logger.setLevel(SCANNER_LOG_LEVEL)

    cache = Cache()
    collector = DNSScanner(DNS_OPTIONS, scanner_child_logger, cache)

    # Main message processing loop
    # dn is the domain name, req is the optional ZoneRequest object
    async for dn, req in stream.items():
        try:
            logger.k_trace("Processing DNS", dn)
            req = ensure_model(DNSRequest, req)

            result = await collector.scan_dns(dn, req)
            logger.k_trace("DNS done", dn)

            await topic_processed_dns.send(key=dn, value=result)

            if result.status_code == 0:
                if result.ips is not None and len(result.ips) > 0:
                    for ip in result.ips:
                        ip_to_process = IPToProcess(ip=ip.ip, domain_name=dn)
                        logger.k_trace("Sending IP %s to process", dn, ip.ip)
                        await topic_ip_requests.send(key=ip_to_process, value=None)

                ip_for_tls = get_ip_for_tls(result.dns_data)
                if ip_for_tls is not None:
                    logger.k_trace("Sending TLS request", dn)
                    await topic_tls_requests.send(key=dn, value=ip_for_tls)
        except Exception as e:
            logger.k_unhandled_error(e, dn)
            await handle_top_level_exception(e, COMPONENT_NAME, dn, DNSResult, topic_processed_dns)
