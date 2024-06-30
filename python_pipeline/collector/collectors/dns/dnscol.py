from dns.resolver import Cache

from collectors.dns.collector import DNSCollector
from collectors.options import DNSCollectorOptions
from collectors.util import handle_top_level_component_exception
from common import read_config, make_app, log
from common.models import DNSRequest, DNSResult, IPToProcess, DNSData
from common.util import ensure_model

COLLECTOR = "dns"
logger = log.get(COLLECTOR)

# Read the config
config = read_config()
component_config = config.get(COLLECTOR, {})

DNS_SERVERS = component_config.get("dns_servers", ['195.113.144.194', '193.17.47.1',
                                                   '195.113.144.233', '185.43.135.1'])
TIMEOUT = component_config.get("timeout", 5)
ROTATE_NAMESERVERS = component_config.get("rotate_nameservers", False)
TYPES_TO_SCAN = component_config.get("types_to_scan", ['A', 'AAAA', 'CNAME', 'MX', 'NS', 'TXT'])
TYPES_TO_PROCESS_IPS_FROM = component_config.get("types_to_process_IPs_from", ['A', 'AAAA', 'CNAME'])
MAX_RECORD_RETRIES = component_config.get("max_record_retries", 2)
USE_ONE_SOCKET = component_config.get("use_one_socket", True)
SCANNER_LOG_LEVEL = component_config.get("scanner_log_level", "INFO")
CONCURRENCY = component_config.get("concurrency", 16)

# The Faust application
dns_app = make_app(COLLECTOR, config)

# The input and output topics
topic_to_process = dns_app.topic('to_process_DNS', key_type=str, key_serializer='str')

topic_processed_dns = dns_app.topic('processed_DNS', key_type=str, key_serializer='str')

topic_tls_requests = dns_app.topic('to_process_TLS', key_type=str, key_serializer='str',
                                   value_type=str, value_serializer='str')

topic_ip_requests = dns_app.topic('to_process_IP', allow_empty=True)


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

    options = DNSCollectorOptions(dns_servers=DNS_SERVERS, timeout=TIMEOUT, rotate_nameservers=ROTATE_NAMESERVERS,
                                  types_to_scan=TYPES_TO_SCAN, types_to_process_IPs_from=TYPES_TO_PROCESS_IPS_FROM,
                                  max_record_retries=MAX_RECORD_RETRIES, use_one_socket=USE_ONE_SOCKET)
    cache = Cache()
    collector = DNSCollector(options, scanner_child_logger, cache)

    # Main message processing loop
    # dn is the domain name, req is the optional ZoneRequest object
    async for dn, req in stream.items():
        try:
            logger.k_trace("Processing DNS", dn)
            req = ensure_model(DNSRequest, req)

            result = await collector.scan_dns(dn, req)
            logger.k_debug("DNS done", dn)

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
            await handle_top_level_component_exception(e, COLLECTOR, dn, DNSResult, topic_processed_dns)
