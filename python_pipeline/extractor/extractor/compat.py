"""compat.py: A compatibility transformation that converts the raw data collected by the DomainRadar Collector
into a format that can be used by the "legacy" transformations."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import base64
from datetime import datetime, UTC

import cryptography.x509
from whoisit import Bootstrap
from whoisit.errors import ParseError
from whoisit.parser import ParseIPNetwork, ParseDomain
from whois.parser import WhoisEntry

from cryptography.x509 import Certificate
from common import get_safe


# omitted:
# - label, category
# - dns_dnssec, dns_has_dnskey, dns_zone_dnskey_selfsign_ok (DNSSEC not collected)
# - tls_evaluated_on (== dns_evaluated_on)
# added: ip_data.nerd_rep
# ip_data.geo.country -> country_code
# ip_data.remarks.average_rtt -> ip_data.average_rtt
# ip_data.rdap.network is an IPvXNetwork object
# rdap dates are always UTC
# flattened geo data lists present already (previously done in a transformation)
# only one of SOA and zone_SOA is always present
# certificates are represented directly as cryptography's Certificate; or str (parse errors)

class CompatibilityTransformation:
    def __init__(self):
        self.whoisit_bootstrap = Bootstrap(allow_insecure_ssl=True)
        try:
            self.whoisit_bootstrap.bootstrap(True, True)
            # TODO: save bootstrap data?
        except Exception as e:
            print(f"Failed to bootstrap the whoisit library: {e}")

    def transform(self, data: dict) -> dict:
        if data.get("invalid_data"):
            return data

        dns_data = get_safe(data, "dnsResult.dnsData") or {}
        rdap_result = data.get("rdapDomainResult", {}) or {}

        rdap_data: dict | None = rdap_result.get("rdapData")
        rdap_entities: list | None = rdap_result.get("entities")
        whois_parsed: dict | None = rdap_result.get("whoisParsed")
        whois_raw: str | None = rdap_result.get("whoisRaw")

        soa, zone_soa = self._make_soa(data)
        rdap_parsed = self._parse_rdap_dn(data["domain_name"], rdap_data, rdap_entities, whois_parsed, whois_raw)
        ip_data = self._make_ip_data(data)
        country_codes, latitudes, longitudes = self._flatten_ip_data(ip_data)

        reg_date = self._ensure_utc_datetime(rdap_parsed.get("registration_date"))
        exp_date = self._ensure_utc_datetime(rdap_parsed.get("expiration_date"))
        last_changed_date = self._ensure_utc_datetime(rdap_parsed.get("last_changed_date"))

        if reg_date is None or exp_date is None or last_changed_date is None:
            reg_date = exp_date = last_changed_date = datetime.fromtimestamp(0, UTC)

        res = {
            "domain_name": data["domain_name"],
            "dns_email_extras": self._make_email_extras(data),
            "dns_ttls": dns_data.get("ttlValues", None),
            "dns_zone": get_safe(data, "zone.zone"),
            "dns_SOA": soa,
            "dns_zone_SOA": zone_soa,
            "dns_A": dns_data.get("A", None),
            "dns_AAAA": dns_data.get("AAAA", None),
            "dns_TXT": dns_data.get("TXT", None),
            "dns_NS": self._make_ns(dns_data),
            "dns_MX": self._make_mx(dns_data),
            "dns_CNAME": get_safe(dns_data, "CNAME.value"),
            "tls": self._make_tls(data),
            "dns_evaluated_on": get_safe(data, "dnsResult.lastAttempt"),
            "rdap_evaluated_on": get_safe(data, "rdapDomainResult.lastAttempt"),
            "rdap_registration_date": reg_date,
            "rdap_expiration_date": exp_date,
            "rdap_last_changed_date": last_changed_date,
            "rdap_dnssec": self._make_rdap_dnssec(rdap_data),
            "rdap_entities": rdap_parsed.get("entities"),
            "ip_data": ip_data,
            "countries": country_codes,
            "latitudes": latitudes,
            "longitudes": longitudes
        }

        return res

    @staticmethod
    def _ensure_utc_datetime(dt: datetime | None) -> datetime | None:
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)

    @staticmethod
    def _flatten_ip_data(ip_data: list):
        country_codes = []
        latitudes = []
        longitudes = []

        for ip in ip_data:
            if ip["geo"] is None:
                continue
            country_codes.append(ip['geo']['country_code'])
            latitudes.append(ip['geo']['latitude'])
            longitudes.append(ip['geo']['longitude'])

        return country_codes, latitudes, longitudes

    @staticmethod
    def _make_email_extras(data: dict) -> dict:
        ret = {
            "spf": False,
            "dkim": False,
            "dmarc": False
        }

        txt = get_safe(data, "dnsResult.dnsData.TXT")
        if not isinstance(txt, list):
            return ret
        for record in txt:
            record_norm = record.lower()
            if "v=spf" in record_norm:
                ret["spf"] = True
            if "v=dkim" in record_norm:
                ret["dkim"] = True
            if "v=dmarc" in record_norm:
                ret["dmarc"] = True

        return ret

    @staticmethod
    def _make_soa(data: dict) -> (dict | None, dict | None):
        soa = get_safe(data, "zone.soa")
        if soa is None:
            return None, None

        soa = {
            "primary_ns": soa["primaryNs"],
            "resp_mailbox_dname": soa["respMailboxDname"],
            "serial": soa["serial"],
            "refresh": soa["refresh"],
            "retry": soa["retry"],
            "expire": soa["expire"],
            "min_ttl": soa["minTTL"]
        }

        dn = data["domain_name"]
        if dn == get_safe(data, "zone.zone"):
            return soa, None
        else:
            return None, soa

    @staticmethod
    def _make_ns(dns_data: dict) -> list | None:
        ns = dns_data.get("NS")
        if ns is None:
            return None

        return [x["nameserver"] for x in ns]

    @staticmethod
    def _make_mx(dns_data: dict) -> list | None:
        mx = dns_data.get("MX")
        if mx is None:
            return None

        return [{"name": x["value"], "priority": x["priority"]} for x in mx]

    @staticmethod
    def _make_tls(data: dict) -> dict | None:
        def make_certificate(cert_data: dict) -> Certificate | str:
            data_b64: str = cert_data.get("derData", "")
            dn: str = cert_data.get("dn", "")

            if len(data_b64) == 0:
                return dn

            # noinspection PyBroadException
            try:
                data_der = base64.b64decode(data_b64)
                cert = cryptography.x509.load_der_x509_certificate(data_der)
            except Exception:
                return dn

            return cert

        tls = get_safe(data, "tlsResult.tlsData")
        if tls is None:
            return None

        certs = tls["certificates"]
        return {
            "cipher": tls["cipher"],
            "protocol": tls["protocol"],
            "count": len(certs),
            "certificates": [make_certificate(x) for x in certs]
        }

    @staticmethod
    def _parse_rdap_backup(rdap_data: dict):
        return {}  # TODO

    @staticmethod
    def _parse_whois_to_rdap_equivalent(dn: str, whois_parsed: dict, whois_raw: str):
        reg_date = exp_date = last_changed_date = None

        if whois_parsed is not None:
            reg_date = whois_parsed.get("created_date", None)
            exp_date = whois_parsed.get("expires_date", None)
            last_changed_date = whois_parsed.get("updated_date", None)

        has_all = reg_date is not None and exp_date is not None and last_changed_date is not None
        if not has_all and whois_raw:
            # noinspection PyBroadException
            try:
                entry = WhoisEntry.load(dn, whois_raw)

                if not reg_date:
                    reg_date = entry.get("creation_date", None)
                if not exp_date:
                    exp_date = entry.get("expiration_date", None)
                if not last_changed_date:
                    last_changed_date = entry.get("updated_date", None)
            except Exception:
                pass

        return {
            "entities": [],  # TODO: WHOIS entities
            "registration_date": reg_date,
            "expiration_date": exp_date,
            "last_changed_date": last_changed_date
        }

    def _parse_rdap_dn(self, dn: str, rdap_data: dict | None, rdap_entities: list | None,
                       whois_parsed: dict | None, whois_raw: str | None) -> dict:
        if rdap_data is not None:
            rdap_data["entities"] = rdap_entities or []
            rdap_parser = ParseDomain(self.whoisit_bootstrap, rdap_data, dn, True)
            try:
                return rdap_parser.parse()
            except ParseError:
                return self._parse_rdap_backup(rdap_data)
        elif whois_parsed is not None or whois_raw is not None:
            return self._parse_whois_to_rdap_equivalent(dn, whois_parsed, whois_raw or "")
        else:
            return {}

    @staticmethod
    def _make_rdap_dnssec(rdap_data: dict | None) -> bool | None:
        if rdap_data is None:
            return None

        return rdap_data.get('secureDNS', {}).get('delegationSigned', None)

    @staticmethod
    def _make_ip_average_rtt(results_for_ip: dict) -> float:
        count = 0
        total = 0
        for collector, results in results_for_ip.items():
            if collector.startswith("rtt") and results["statusCode"] == 0:
                data = results.get("data", {})
                col_count = data.get("received", 0)
                count += col_count
                total += data.get("avg", 0) * col_count

        return total / count if count > 0 else 0.0

    def _make_ip_data(self, data: dict) -> list[dict]:
        def t_ip_ver(ver: str | None):
            return "6" if ver == "v6" else ("4" if ver == "v4" else ver)

        ips = get_safe(data, "dnsResult.ips")
        ip_results = get_safe(data, "dnsResult.ipResults") or {}
        ret = []
        for ip_obj in ips:
            ip = ip_obj["ip"]
            collectors_results = ip_results.get(ip, {})
            geo_asn = collectors_results.get("geo_asn", {}).get("data", {})
            rdap = collectors_results.get("rdap_ip", {}).get("data", {})
            rdap_parsed = {}
            if len(rdap) != 0:
                rdap_parser = ParseIPNetwork(self.whoisit_bootstrap, rdap, ip, True)
                try:
                    rdap_parsed = rdap_parser.parse()
                except ParseError:
                    pass

            network_address: str | None = geo_asn.get("networkAddress", None)
            if network_address is not None and len(network_address) > 0 and network_address[0] == '/':
                network_address = network_address[1:]

            new_ip = {
                "ip": ip,
                "from_record": ip_obj["type"],
                "asn": {
                    "asn": geo_asn.get("asn", None),
                    "as_org": geo_asn.get("asnOrg", None),
                    "network_address": network_address,
                    "prefix_len": geo_asn.get("prefixLength", None)
                } if "asn" in geo_asn else None,
                "rdap": {
                    "ip_version": t_ip_ver(rdap.get("ipVersion", None)),
                    "entities": rdap_parsed.get("entities", None),
                    "network": rdap_parsed.get("network", None)
                } if ("ipVersion" in rdap) or ("entities" in rdap_parsed) else None,
                "geo": {
                    "country_code": geo_asn.get("countryCode", None),
                    "latitude": geo_asn.get("latitude", None),
                    "longitude": geo_asn.get("longitude", None)
                } if "countryCode" in geo_asn else None,
                "average_rtt": self._make_ip_average_rtt(collectors_results),
                "nerd_rep": get_safe(collectors_results, "nerd.data.reputation") or -1
            }

            ret.append(new_ip)

        return ret
