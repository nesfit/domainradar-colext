from logging import Logger
import socket

import dns
import dns.resolver
import dns.rdatatype as rdt
import tldextract

from .datatypes.dns import ZoneInfo, SOARecord


class DNSCollectorOptions:
    def __init__(self, **kwargs):
        # CESNET, CZ.NIC, CESNET, CZ.NIC
        self.dns_servers = kwargs.get('dns_servers') or ['195.113.144.194', '193.17.47.1',
                                                         '195.113.144.233', '185.43.135.1']
        self.collect_ips_from = kwargs.get('collect_ips_from') or ('A', 'AAAA', 'CNAME', 'MX')
        self.timeout = kwargs.get('timeout') or 3


class DNSCollector:
    def __init__(self, options: DNSCollectorOptions, logger: Logger, cache: dns.resolver.Cache | None = None):
        self._options = options
        self._logger = logger

        self._dns = dns.resolver.Resolver()
        self._dns.nameservers = options.dns_servers
        self._dns.lifetime = options.timeout
        self._dns.cache = cache

        self._udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._udp_sock.setblocking(False)

    def __del__(self):
        if self._udp_sock:
            self._udp_sock.close()
            self._udp_sock = None

    def get_zone_info(self, domain_name: str) -> ZoneInfo | None:
        """
        Finds the zone domain name and SOA record of the most specific zone (deepest point of delegation)
        in which the input domain name resides.

        Returns:
        - None, if the input is empty or an IP address, has invalid suffix (eTLD), or no active zone was found.
        - ZoneInfo, if the zone domain name and SOA record were found.
          - When domain_name contains an eTLD (e.g. 'cz', 'co.uk' or 'hakodate.hokkaido.jp'), the resolution is
            performed so the result is the SOA record of the eTLD.
          - However, if domain_name is any other DN, the eTLD is skipped (e.g. for 'fit.vut.cz', the query is made
            for 'vut.cz' and 'fit.vut.cz' but not 'cz').
        """
        name_parts = tldextract.extract(domain_name)

        if name_parts.ipv4 or name_parts.ipv6:
            # The input is an IP address
            return None

        soa_record = None

        if name_parts.domain != '' and name_parts.suffix != '':
            # Normal case, domain and suffix are present
            domain = "." + name_parts.fqdn
            from_dot_index = domain.rindex('.', 0, len(domain) - len(name_parts.suffix) - 1)
        elif name_parts.domain != '' and name_parts.suffix == '':
            # Invalid suffix, the domain name cannot exist in global DNS
            return None
        elif name_parts.domain == '' and name_parts.suffix != '':
            # Only suffix, the domain name is an eTLD, process it as a domain
            domain = "." + name_parts.suffix
            from_dot_index = domain.rindex('.', 0, len(domain) - 1)
        else:
            # Neither domain nor suffix found, invalid input
            return None

        zone = None
        while True:
            domain_to_check = domain[from_dot_index + 1:]
            try:
                answer = self._dns.resolve(domain_to_check, rdt.SOA)
                if len(answer) == 0 or answer[0].rdtype != rdt.SOA:
                    break

                soa = answer[0]  # type: dns.rdtypes.ANY.SOA.SOA
                soa_record = SOARecord(soa.mname.to_text(True), soa.rname.to_text(True),
                                       soa.serial, soa.refresh,
                                       soa.retry, soa.expire, soa.minimum)
                zone = domain_to_check
            except dns.resolver.NoAnswer:
                pass
            except dns.resolver.NXDOMAIN:
                break

            if from_dot_index <= 0:
                break

            from_dot_index = domain.rindex('.', 0, from_dot_index - 1)

        if zone is None:
            # No active zone found
            return None

        primary_ns_ips = self._resolve_ips(soa_record.primary_ns)

        nameserver_ips = set()
        nameservers = self._find_nameservers(zone)
        # Remove the primary NS from the set of nameservers
        nameservers.discard(soa_record.primary_ns)

        for nameserver in nameservers:
            nameserver_ips.update(self._resolve_ips(nameserver))

        # Remove the primary NS IPs from the set of nameserver IPs
        nameserver_ips.difference_update(primary_ns_ips)

        return ZoneInfo(soa_record, zone, primary_ns_ips, nameservers, nameserver_ips)

    def _find_nameservers(self, domain_name: str) -> set[str]:
        """
        Uses the general resolver to find NS records of the input domain name.
        Returns a set of nameserver domain names.
        """
        try:
            answer = self._dns.resolve(domain_name, rdt.NS)
            return set(x.target.to_text(True) for x in answer if x.rdtype == rdt.NS)
        except dns.resolver.NoAnswer:
            return set()
        except dns.resolver.NXDOMAIN:
            return set()

    def _resolve_ips(self, domain_name: str) -> set[str]:
        """
        Uses the general resolver to find IPs in A and AAAA records of the input domain name.
        Returns a set of all found IP addresses.
        """
        ips = set()
        for rtype in (rdt.A, rdt.AAAA):
            try:
                answer = self._dns.resolve(domain_name, rtype)
                ips.update(x.address for x in answer if x.rdtype == rtype)
            except dns.resolver.NoAnswer:
                pass
            except dns.resolver.NXDOMAIN:
                pass

        return ips
