from logging import getLogger
from json import dumps

from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters

from .datatypes.dns import ZoneInfo, SOARecord
from .dns import DNSCollector, DNSCollectorOptions


class DNSCollectorWrapper:
    class Java:
        implements = ["cz.vut.fit.domainradar.python.PythonEntryPoint"]

    def __init__(self):
        options = DNSCollectorOptions()
        self._dns = DNSCollector(options, getLogger("dns_collector_wrapper"), None)

    def getZoneInfo(self, domainName: str) -> str | None:
        result = self._dns.get_zone_info(domainName)
        if result is None:
            return None

        result_obj = {
            "soa": {
                "primaryNs": result.soa.primary_ns,
                "respMailboxDname": result.soa.resp_mailbox_dname,
                "serial": str(result.soa.serial),
                "refresh": result.soa.refresh,
                "retry": result.soa.retry,
                "expire": result.soa.expire,
                "minTTL": result.soa.min_ttl
            } if result.soa else None,
            "zone": result.zone,
            "primaryNameserverIps": list(result.primary_nameserver_ips),
            "secondaryNameservers": list(result.secondary_nameservers),
            "secondaryNameserverIps": list(result.secondary_nameservers_ips)
        }

        return dumps(result_obj)


wrapper = DNSCollectorWrapper()

gateway = JavaGateway(
    gateway_parameters=GatewayParameters(
        auto_convert=True
    ),
    callback_server_parameters=CallbackServerParameters(),
    python_server_entry_point=wrapper
)

# Keep the gateway running
gateway.shutdown_on_interrupt()
