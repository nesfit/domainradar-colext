"""dnscol.py: The processor for the DNS collector."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from dns.resolver import Cache

from collectors.dns.scanner import DNSScanner
from collectors.dns_options import DNSCollectorOptions
from collectors.util import make_top_level_exception_result
from common import log
from common.models import DNSRequest, DNSResult, IPToProcess, DNSData
from common.util import ensure_model, dump_model
from domrad_kafka_client import AsyncProcessorBase, SimpleMessage

COLLECTOR = "dns"
COMPONENT_NAME = "collector-" + COLLECTOR


class DNSProcessor(AsyncProcessorBase):
    def __init__(self, config: dict):
        super().__init__(config)
        self._logger = log.init("worker")

        component_config = config.get(COLLECTOR, {})
        self._dns_options = DNSCollectorOptions.from_config(component_config)

        cache = Cache()
        scanner_child_logger = self._logger.getChild("scanner")
        scanner_child_logger.setLevel(component_config.get("scanner_log_level", "INFO"))

        self._collector = DNSScanner(self._dns_options, scanner_child_logger, cache)

    async def process(self, key: bytes, value: bytes, partition: int, offset: int) -> list[SimpleMessage]:
        logger = self._logger
        dn = None
        ret = []

        try:
            dn = key.decode("utf-8")
            logger.k_trace("Processing DNS", dn)
            req = ensure_model(DNSRequest, value)

            result = await self._collector.scan_dns(dn, req)
            logger.k_trace("DNS done", dn)

            ret.append(('processed_DNS', key, dump_model(result)))

            if result.status_code == 0:
                if result.ips is not None and len(result.ips) > 0:
                    for ip in result.ips:
                        ip_to_process = IPToProcess(ip=ip.ip, domain_name=dn)
                        logger.k_trace("Sending IP %s to process", dn, ip.ip)
                        ret.append(('to_process_IP', dump_model(ip_to_process), None))

                ip_for_tls = self.get_ip_for_tls(result.dns_data)
                if ip_for_tls is not None:
                    logger.k_trace("Sending TLS request", dn)
                    ret.append(('to_process_TLS', key, dump_model(ip_for_tls)))

        except Exception as e:
            logger.k_unhandled_error(e, dn)
            ret.append(make_top_level_exception_result('processed_DNS', e, COMPONENT_NAME, dn, DNSResult))

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
