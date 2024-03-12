from typing import Optional, Dict, List, NamedTuple


class IPFromDNS(NamedTuple):
    ip: str
    source_record_type: str


class IPRecord(NamedTuple):
    ttl: int = -1
    value: str = ''


class CNAMERecord(NamedTuple):
    value: str = ''
    related_ips: List[IPRecord] = []


class MXRecord(NamedTuple):
    priority: int = -1
    related_ips: List[IPRecord] = []


class NSRecord(NamedTuple):
    related_ips: List[IPRecord] = []


class SOARecord(NamedTuple):
    primary_ns: str = ''
    resp_mailbox_dname: str = ''
    serial: str = ''
    refresh: int = -1
    retry: int = -1
    expire: int = -1
    min_ttl: int = -1


class ZoneInfo(NamedTuple):
    soa: SOARecord
    zone: str = ''
    primary_nameserver_ips: set[str] = set()
    secondary_nameservers: set[str] = set()
    secondary_nameservers_ips: set[str] = set()


class RecordsIntMetadata(NamedTuple):
    A: int = -1
    AAAA: int = -1
    CNAME: int = -1
    MX: int = -1
    NS: int = -1
    SOA: int = -1
    TXT: int = -1


DNSSECMetadata = RecordsIntMetadata
RecordSourceMetadata = RecordsIntMetadata
TTLMetadata = RecordsIntMetadata


class DNSDataRemarks(NamedTuple):
    has_spf: bool = False
    has_dkim: bool = False
    has_dmarc: bool = False
    has_dnskey: bool = False
    zone_dnskey_selfsign_ok: bool = False
    zone: str = ''


class DNSDataNT(NamedTuple):
    """DNS data structure"""

    dnssec: DNSSECMetadata
    remarks: DNSDataRemarks
    sources: RecordSourceMetadata
    ttls: TTLMetadata

    SOA: Optional[SOARecord]
    zone_SOA: Optional[SOARecord]
    NS: Optional[Dict[str, NSRecord]]
    A: Optional[List[str]]
    AAAA: Optional[List[str]]
    CNAME: Optional[CNAMERecord]
    MX: Optional[Dict[str, MXRecord]]
    TXT: Optional[List[str]]

    @classmethod
    def from_typed_dict(cls, td):
        return cls(
            dnssec=DNSSECMetadata(**td['dnssec']) if 'dnssec' in td else None,
            remarks=DNSDataRemarks(**td['remarks']) if 'remarks' in td else None,
            sources=RecordSourceMetadata(**td['sources']) if 'sources' in td else None,
            ttls=TTLMetadata(**td['ttls']) if 'ttls' in td else None,
            SOA=SOARecord(**td['SOA']) if 'SOA' in td and td['SOA'] else None,
            zone_SOA=SOARecord(**td['zone_SOA']) if 'zone_SOA' in td and td['zone_SOA'] else None,
            NS={k: NSRecord(**v) for k, v in td['NS'].items()} if 'NS' in td and td['NS'] else None,
            A=td['A'] if 'A' in td else None,
            AAAA=td['AAAA'] if 'AAAA' in td else None,
            CNAME=CNAMERecord(**td['CNAME']) if 'CNAME' in td and td['CNAME'] else None,
            MX={k: MXRecord(**v) for k, v in td['MX'].items()} if 'mx' in td and td['MX'] else None,
            TXT=td['TXT'] if 'TXT' in td else None
        )
