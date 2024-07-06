import ipaddress
import socket
from logging import Logger
from typing import Optional, Literal, Any

import dns
import dns.asyncquery
import dns.asyncresolver
import dns.dnssec
import dns.rdatatype as rdt
import dns.resolver
from dns.message import Message
from dns.name import Name
from dns.rrset import RRset

from collectors.options import DNSCollectorOptions
from common import result_codes as rc
from common.models import DNSData, CNAMERecord, MXRecord, NSRecord, IPFromRecord, DNSRequest, DNSResult


class DNSCollector:
    def __init__(self, options: DNSCollectorOptions, logger: Logger, cache: dns.resolver.Cache | None = None):
        self._options = options
        self._logger = logger

        self._dns = dns.asyncresolver.Resolver(configure=False)
        self._dns.nameservers = options.dns_servers
        self._dns.rotate = options.rotate_nameservers
        self._dns.timeout = options.timeout
        self._dns.lifetime = options.timeout * 1.2
        self._dns.cache = cache
        self._udp_sock = None

        self._timeout_error = f"Timeout ({options.timeout} s)"

    async def scan_dns(self, domain_name: str, request: DNSRequest) -> DNSResult:
        if self._options.use_one_socket and self._udp_sock is None:
            self._udp_sock = await dns.asyncbackend.get_default_backend().make_socket(socket.AF_INET, socket.SOCK_DGRAM)

        zone = request.zone_info
        try:
            domain = dns.name.from_text(domain_name)
            dns.name.from_text(zone.zone)
        except dns.exception.SyntaxError as e:
            return DNSResult(status_code=rc.INVALID_DOMAIN_NAME, error=str(e))

        primary_ns_ips = list(zone.primary_nameserver_ips)

        types = request.dns_types_to_collect
        if types is None or len(types) == 0:
            types = self._options.types_to_scan

        adr_types = request.dns_types_to_process_IPs_from
        if adr_types is None:
            adr_types = self._options.types_to_process_IPs_from

        ret = dict()
        ret["ttls"] = dict()
        ret["errors"] = dict()
        ret_ips = list()

        if 'A' in types:
            await self._resolve_a_or_aaaa(domain, 'A', primary_ns_ips, ret, ret_ips, 'A' in adr_types)
        if 'AAAA' in types:
            await self._resolve_a_or_aaaa(domain, 'AAAA', primary_ns_ips, ret, ret_ips, 'AAAA' in adr_types)
        if 'CNAME' in types:
            await self._resolve_cname(domain, primary_ns_ips, ret, ret_ips, 'CNAME' in adr_types)
        if 'MX' in types:
            await self._resolve_mx(domain, primary_ns_ips, ret, ret_ips, 'MX' in adr_types)
        if 'NS' in types:
            await self._resolve_ns(domain, primary_ns_ips, ret, ret_ips, 'NS' in adr_types)
        if 'TXT' in types:
            await self._resolve_txt(domain, primary_ns_ips, ret)

        ret_errors = ret['errors']
        if len(ret_errors) == 0:
            ret_errors = None
        elif len(ret_errors) == len(types):
            # All queries failed, return an erroneous result
            # Check if all errors are timeouts
            for error_str in ret_errors:
                if error_str != self._timeout_error:
                    # At least one error is not a timeout
                    return DNSResult(status_code=rc.OTHER_DNS_ERROR,
                                     dns_data=DNSData(errors=ret_errors),
                                     error="All queries failed")
            return DNSResult(status_code=rc.TIMEOUT,
                             error=f"All queries timed out ({self._timeout_error} s)")

        dns_data = DNSData(
            a=ret.get('A'),
            aaaa=ret.get('AAAA'),
            cname=ret.get('CNAME'),
            mx=ret.get('MX'),
            ns=ret.get('NS'),
            txt=ret.get('TXT'),
            ttl_values=ret.get('ttls', {}),
            errors=ret_errors
        )

        return DNSResult(status_code=0, dns_data=dns_data, ips=ret_ips)

    async def _resolve_a_or_aaaa(self, domain: Name, record_type: Literal['A', 'AAAA'], primary_ns: list[str],
                                 result: dict, ips: list[IPFromRecord], add_ips: bool):
        """Resolves an A or AAAA record set for a given domain name and populates a result object."""
        data = await self._resolve_record_base(domain, record_type, primary_ns, result)
        if data is None:
            return

        result[record_type] = []
        for a_record in data:  # type: dns.rdtypes.IN.A.A
            result[record_type].append(a_record.address)
            if add_ips:
                ips.append(IPFromRecord(ip=a_record.address, type=record_type))

    async def _resolve_cname(self, domain: Name, primary_ns: list[str], result: dict, ips: list[IPFromRecord],
                             add_ips: bool):
        """Resolves a CNAME record for a given domain name and populates a result object."""
        data = await self._resolve_record_base(domain, 'CNAME', primary_ns, result)
        if data is None:
            return

        if len(data) > 1:
            self._logger.warning(f"Multiple CNAME records for {domain}")

        value = data[0].target  # type: Name
        related_ips = await self._resolve_ips(value)
        result['CNAME'] = CNAMERecord(value=value.to_text(True), related_ips=related_ips)
        if add_ips:
            for related_ip in related_ips:
                ips.append(IPFromRecord(ip=related_ip, type='CNAME'))

    async def _resolve_mx(self, domain: Name, primary_ns: list[str], result: dict, ips: list[IPFromRecord],
                          add_ips: bool):
        """Resolves an MX record set for a given domain name and populates a result object."""
        data = await self._resolve_record_base(domain, 'MX', primary_ns, result)
        if data is None:
            return

        result['MX'] = []
        for mx_record in data:  # type: dns.rdtypes.ANY.MX.MX
            related_ips = await self._resolve_ips(mx_record.exchange)
            result['MX'].append(MXRecord(value=mx_record.exchange.to_text(True), priority=mx_record.preference,
                                         related_ips=related_ips))
            if add_ips:
                for related_ip in related_ips:
                    ips.append(IPFromRecord(ip=related_ip, type='MX'))

    async def _resolve_ns(self, domain: Name, primary_ns: list[str], result: dict, ips: list[IPFromRecord],
                          add_ips: bool):
        """Resolves a NS record set for a given domain name and populates a result object."""
        data = await self._resolve_record_base(domain, 'NS', primary_ns, result)
        if data is None:
            return

        result['NS'] = []
        for ns_record in data:  # type: dns.rdtypes.ANY.NS.NS
            related_ips = await self._resolve_ips(ns_record.target)
            result['NS'].append(NSRecord(nameserver=ns_record.target.to_text(True), related_ips=related_ips))
            if add_ips:
                for related_ip in related_ips:
                    ips.append(IPFromRecord(ip=related_ip, type='NS'))

    async def _resolve_txt(self, domain: Name, primary_ns: list[str], result: dict):
        """
        Resolves a TXT record set for a given domain name and populates a result object.
        Checks the TXT records for known values, such as SPF, DKIM and DMARC control strings.
        """
        data = await self._resolve_record_base(domain, 'TXT', primary_ns, result)
        if data is None:
            return

        result['TXT'] = []
        for txt_record in data:  # type: dns.rdtypes.ANY.TXT.TXT
            for string in txt_record.strings:
                text_orig = string.decode()
                result['TXT'].append(text_orig)

    async def _resolve_record_base(self, domain: Name, record_type: str, primary_ns: list[str],
                                   result: dict[str, Any]) -> Optional[RRset]:
        """
        Common base for record resolving. Populates the corresponding DNSSEC, TTL and source of resolution metadata
        values in a result object. Consumes exceptions, returns None when there's an error, the resulting RRset
        doesn't match the queried domain name or when it's empty.
        """
        # noinspection PyBroadException
        try:
            data, _, from_primary, err = await self._resolve(domain, record_type, primary_ns)
            if data is None or data.name != domain:
                if err is not None:
                    result['errors'][record_type] = err
                return None

            result['ttls'][record_type] = data.ttl
            if len(data) == 0:
                result['errors'][record_type] = f"Got answer but no record data"  # Shouldn't happen
                return None

            return data
        except Exception as e:
            result['errors'][record_type] = str(e)

    async def _resolve(self, domain: Name, record_type: str, primary_ns: Optional[list[str]]) -> \
            tuple[Optional[RRset], Optional[RRset], bool, Optional[str]]:
        """
        Queries a record set of a given type for a domain. Tries to use provided IP addresses of the primary nameserver.
        When a query to a primary NS fails, the address is removed from the provided list. When no addresses are left,
        uses dnspython's stub resolver with the globally configured DNS server address(es).
        """

        # noinspection PyShadowingNames
        def get_response_pair(response: Message) -> tuple[RRset, Optional[RRset]]:
            """Extracts the RRset bearing the queried data, and the signature RRset"""
            record_type_num = dns.rdatatype.from_text(record_type)
            data_set = None
            rrsig_set = None

            for rrset in response.answer:
                if rrset.rdtype == record_type_num:
                    data_set = rrset
                if rrset.rdtype == dns.rdatatype.RRSIG:
                    rrsig_set = rrset

            if data_set is None:
                raise KeyError(f"{record_type} record not found in response")

            return data_set, rrsig_set

        fallback = False
        if primary_ns is None or len(primary_ns) == 0:
            fallback = True

        retries_left = self._options.max_record_retries
        nameserver_count_threshold = max(1, len(primary_ns) // 2)

        while not fallback:
            if len(primary_ns) == 0:
                break

            ns_to_try = primary_ns[0]
            try:
                if ipaddress.ip_address(ns_to_try).version == 6:
                    del primary_ns[0]
                    continue
            except ValueError:
                del primary_ns[0]
                continue

            query = dns.message.make_query(domain, record_type, use_edns=True, want_dnssec=True)
            # noinspection PyBroadException
            try:
                response, _ = await dns.asyncquery.udp_with_fallback(query, ns_to_try, self._options.timeout,
                                                                     udp_sock=self._udp_sock)
                res_data, res_sig = get_response_pair(response)
                return res_data, res_sig, True, None
            except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, KeyError):
                self._logger.debug(f"{domain}: {record_type} not found (primary NS {ns_to_try})")
                fallback = True
            except dns.exception.Timeout:
                retries_left -= 1
                if retries_left == 0:
                    return None, None, True, self._timeout_error
                else:
                    self._logger.debug(
                        f"{domain}: {record_type} timeout, {retries_left} retries left (pNS {ns_to_try})")
                    if len(primary_ns) > nameserver_count_threshold:
                        del primary_ns[0]
                    else:
                        break
            except Exception as e:
                self._logger.info(f"{domain}: {record_type} error (pNS {ns_to_try}): {str(e)}")
                return None, None, True, str(e)

        # noinspection PyBroadException
        try:
            answer = await self._dns.resolve(domain, record_type)
            res_data, res_sig = get_response_pair(answer.response)
            return res_data, res_sig, False, None
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, KeyError):
            self._logger.debug(f"{domain}: {record_type} not found (fallback NS)")
            return None, None, False, None
        except dns.exception.Timeout:
            self._logger.debug(f"{domain}: {record_type} timeout (fallback NS)")
            return None, None, False, self._timeout_error
        except Exception as e:
            self._logger.info(f"{domain}: {record_type} error (fallback NS): {str(e)}")
            return None, None, False, str(e)

    async def _resolve_ips(self, domain_name: str | Name) -> set[str]:
        """
        Uses the general resolver to find IPs in A and AAAA records of the input domain name.
        Returns a set of all found IP addresses.
        """
        ips = set()
        for rtype in (rdt.A, rdt.AAAA):
            try:
                answer = await self._dns.resolve(domain_name, rtype)
                ips.update(x.address for x in answer if x.rdtype == rtype)
            except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
                pass
            except dns.exception.Timeout:
                self._logger.debug(f"{domain_name}: IP resolution timeout (fallback NS)")
                pass

        return ips
