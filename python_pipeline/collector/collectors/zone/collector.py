from logging import Logger

import dns
import dns.asyncresolver
import dns.rdatatype as rdt
import tldextract

from collectors.options import DNSCollectorOptions
from common.models import ZoneInfo, SOARecord


class ZoneCollector:
    def __init__(self, options: DNSCollectorOptions, logger: Logger, cache: dns.resolver.Cache | None = None):
        self._options = options
        self._logger = logger

        self._dns = dns.asyncresolver.Resolver(configure=False)
        self._dns.nameservers = options.dns_servers
        self._dns.rotate = options.rotate_nameservers
        self._dns.timeout = options.timeout
        self._dns.lifetime = options.timeout * 1.2
        self._dns.cache = cache

    async def get_zone_info(self, domain_name: str) -> ZoneInfo | None:
        """
        Finds the zone domain name and SOA record of the most specific zone (deepest point of delegation)
        in which the input domain name resides.

        * When domain_name contains an eTLD (e.g. 'cz', 'co.uk' or 'hakodate.hokkaido.jp'), the resolution is
          performed so the result is the SOA record of the eTLD.
        * However, if domain_name is any other DN, the eTLD is skipped (e.g. for 'fit.vut.cz', the query is made
          for 'vut.cz' and 'fit.vut.cz' but not 'cz').

        :raises dns.resolver.Timeout: if any DNS query in the process of determining the zone timed out.
        :return: None if the input is empty or an IP address, has invalid suffix (eTLD), or no active zone was found.
            | :class:`ZoneInfo` if the zone domain name and SOA record were found.
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

        input_name = dns.name.from_text(domain_name)
        zone = None
        while True:
            domain_to_check = domain[from_dot_index + 1:]
            try:
                answer = await self._dns.resolve(domain_to_check, rdt.SOA)
                if len(answer) == 0 or answer[0].rdtype != rdt.SOA:
                    break
                # Some unicorn DNS servers return a SOA in the Answer section even for CNAMEs... and it points
                # to the SOA of the CNAME target. We need to check if the SOA is for the correct domain.
                if input_name.is_subdomain(answer.rrset.name):
                    soa = answer[0]  # type: dns.rdtypes.ANY.SOA.SOA
                    soa_record = SOARecord(primary_ns=soa.mname.to_text(True),
                                           resp_mailbox_dname=soa.rname.to_text(True),
                                           serial=str(soa.serial), refresh=soa.refresh,
                                           retry=soa.retry, expire=soa.expire, min_ttl=soa.minimum)
                    zone = domain_to_check
            except dns.resolver.NoAnswer:
                pass
            except dns.resolver.Timeout:
                raise
            except dns.resolver.NXDOMAIN:
                break

            if from_dot_index <= 0:
                break

            from_dot_index = domain.rindex('.', 0, from_dot_index - 1)

        if zone is None:
            # No active zone found
            return None

        primary_ns_ips = await self._resolve_ips(soa_record.primary_ns)

        nameserver_ips = set()
        nameservers = await self._find_nameservers(zone)
        # Remove the primary NS from the set of nameservers
        nameservers.discard(soa_record.primary_ns)

        for nameserver in nameservers:
            nameserver_ips.update(await self._resolve_ips(nameserver))

        # Remove the primary NS IPs from the set of nameserver IPs
        nameserver_ips.difference_update(primary_ns_ips)

        has_dnskey = await self._has_dnskey(zone)

        return ZoneInfo(soa=soa_record, zone=zone, primary_nameserver_ips=primary_ns_ips,
                        secondary_nameservers=nameservers, secondary_nameserver_ips=nameserver_ips,
                        public_suffix=name_parts.suffix, has_dnskey=has_dnskey)

    async def _has_dnskey(self, domain_name: str) -> bool | None:
        try:
            dnskey_rrset = await self._dns.resolve(domain_name, rdt.DNSKEY)
            return dnskey_rrset is not None and len(dnskey_rrset) != 0 and dnskey_rrset.rdtype == rdt.DNSKEY
        except dns.resolver.NoAnswer:
            return False
        except dns.exception.DNSException:
            return None

    async def _find_nameservers(self, domain_name: str) -> set[str]:
        """
        Uses the general resolver to find NS records of the input domain name.
        Returns a set of nameserver domain names.
        """
        try:
            answer = await self._dns.resolve(domain_name, rdt.NS)
            return set(x.target.to_text(True) for x in answer if x.rdtype == rdt.NS)
        except dns.exception.DNSException:
            return set()

    async def _resolve_ips(self, domain_name: str) -> set[str]:
        """
        Uses the general resolver to find IPs in A and AAAA records of the input domain name.
        Returns a set of all found IP addresses.
        """
        ips = set()
        for rtype in (rdt.A, rdt.AAAA):
            try:
                answer = await self._dns.resolve(domain_name, rtype)
                ips.update(x.address for x in answer if x.rdtype == rtype)
            except dns.exception.DNSException:
                pass

        return ips
