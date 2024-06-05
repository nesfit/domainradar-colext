"""incomplete_data_filter.py: Provides a function for filtering out entries with incomplete data."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from common import get_safe


def filter_entry(entry: dict) -> tuple[bool, dict | None]:
    """Decides whether an entry is complete "enough" to extract features from it and pass to the classifier.
    :param entry: A dictionary that contains the collected data.
    :return: A tuple of a boolean indicating whether the entry is complete enough and a dictionary that describes
    why the entry has been deemed incomplete.
    """
    ret_errors = {}

    dns_status = get_safe(entry, "dnsResult.statusCode")
    dns_ready = dns_status == 0
    if not dns_ready:
        ret_errors["dns"] = "status code " + str(dns_status)

    rdap_dn_ready = entry.get("rdapDomainResult") is not None
    if not rdap_dn_ready:
        ret_errors["rdap_dn"] = "null"

    tls_ready = entry.get("tlsResult") is not None
    if not tls_ready:
        ret_errors["tls"] = "null"

    ips = get_safe(entry, "dnsResult.ips") or []
    ip_data = get_safe(entry, "dnsResult.ipResults") or {}
    ips_with_some_data = 0

    for ip_pair in ips:
        ip = ip_pair.get("ip")
        if ip is None:
            ret_errors["ip." + str(ip_pair)] = "invalid IP pair"
            continue

        if ip not in ip_data:
            ret_errors["ip." + ip] = "no data"
        else:
            ips_with_some_data += 1

    ips_ready = len(ips) == 0 or ips_with_some_data > len(ips) // 2
    if not ips_ready:
        ret_errors["ip"] = f"only got some data for {ips_with_some_data} out of {len(ips)} IPs"

    return dns_ready and rdap_dn_ready and tls_ready and ips_ready, ret_errors
