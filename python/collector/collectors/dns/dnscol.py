"""dnscol.py: The processor for the DNS collector."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from dns.resolver import Cache

from collectors.dns.scanner import DNSScanner
from collectors.dns_options import DNSCollectorOptions
from common import log
from common.models import DNSRequest, IPToProcess, DNSData, DNSResult
from common.util import dump_model
from domrad_kafka_client import SimpleMessage, Message
from collectors.processor import BaseAsyncCollectorProcessor

COLLECTOR = "dns"
COMPONENT_NAME = "collector-" + COLLECTOR


class DNSProcessor(BaseAsyncCollectorProcessor[str, DNSRequest]):
    def __init__(self, config: dict):
        super().__init__(config, COMPONENT_NAME, 'processed_DNS', str, DNSRequest, DNSResult)
        self._logger = log.init("worker")

        component_config = config.get(COLLECTOR, {})
        self._dns_options = DNSCollectorOptions.from_config(component_config)

        cache = Cache()
        scanner_child_logger = self._logger.getChild("scanner")
        scanner_child_logger.setLevel(component_config.get("scanner_log_level", "INFO"))

        self._collector = DNSScanner(self._dns_options, scanner_child_logger, cache)

    async def process(self, message: Message[str, DNSRequest]) -> list[SimpleMessage]:
        logger = self._logger
        dn = message.key
        req = message.value
        ret = []

        logger.k_trace("Processing DNS", dn)
        result = await self._collector.scan_dns(dn, req)
        logger.k_trace("DNS done", dn)

        ret.append((self._output_topic, message.key_raw, dump_model(result)))

        if result.status_code == 0:
            if result.ips is not None and len(result.ips) > 0:
                for ip in result.ips:
                    ip_to_process = IPToProcess(ip=ip.ip, dn=dn)
                    logger.k_trace("Sending IP %s to process", dn, ip.ip)
                    ret.append(('to_process_IP', dump_model(ip_to_process), None))

            ip_for_tls = self.get_ip_for_tls(result.dns_data)
            if ip_for_tls is not None:
                logger.k_trace("Sending TLS request", dn)
                ret.append(('to_process_TLS', message.key_raw, dump_model(ip_for_tls)))

        return ret

    @staticmethod
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
