from typing import Optional

from faust import Record
from faust.models.fields import FieldDescriptor, IntegerField, StringField, BooleanField, FloatField


class IPToProcess(Record, coerce=True, serializer='json'):
    domain_name: str = StringField(field="domain_name", required=True)
    ip: str = StringField(field="ip", required=True)


class Result(Record, abstract=True, coerce=True, serializer='json'):
    status_code: int = IntegerField(field="statusCode", required=True)
    error: Optional[str] = StringField(required=False)
    last_attempt: int = IntegerField(field="lastAttempt")


class IPResult(Result, abstract=True, serializer='json'):
    collector: str = StringField(required=True)


class RDAPIPResult(IPResult, abstract=False):
    data: Optional[dict] = FieldDescriptor[dict](field="data", required=False)


class RTTData(Record):
    min: float = FloatField(required=True)
    avg: float = FloatField(required=True)
    max: float = FloatField(required=True)
    sent: int = IntegerField(required=True)
    received: int = IntegerField(required=True)
    jitter: float = FloatField(required=True)


class RTTResult(IPResult, abstract=False):
    data: Optional[RTTData] = FieldDescriptor[RTTData](field="data", required=False)


class RDAPDomainResult(Result, abstract=False):
    rdap_data: Optional[dict] = FieldDescriptor[dict](field="rdapData", required=False)
    entities: Optional[list[dict]] = FieldDescriptor[list[dict]](required=False)
    is_for_source_name: bool = BooleanField(field="forSourceName", required=True)

    whois_status_code: int = IntegerField(field="whoisStatusCode", required=True)
    whois_error: Optional[str] = StringField(field="whoisError", required=False)
    raw_whois_data: Optional[str] = StringField(field="whoisRaw", required=False)
    parsed_whois_data: Optional[dict] = FieldDescriptor[dict](field="whoisParsed", required=False)


class RDAPRequest(Record, coerce=True, serializer='json'):
    zone: Optional[str] = StringField(required=False)


class SOARecord(Record):
    primary_ns: str = StringField(input_name="primaryNs")
    resp_mailbox_dname: str = StringField(input_name="respMailboxDname")
    serial: str
    refresh: int
    retry: int
    expire: int
    min_ttl: int = IntegerField(input_name="minTTL")


class NSRecord(Record):
    nameserver: str
    related_ips: Optional[list[str]] = FieldDescriptor[list[str]](input_name="relatedIps",
                                                                  required=False)


class CNAMERecord(Record):
    value: str
    related_ips: Optional[list[str]] = FieldDescriptor[list[str]](input_name="relatedIps",
                                                                  required=False)


class MXRecord(CNAMERecord):
    priority: int


class DNSData(Record):
    ttl_values: dict[str, int] = FieldDescriptor[dict[str, int]](input_name="ttlValues")
    a: Optional[list[str]] = FieldDescriptor[list[str]](input_name="A", required=False)
    aaaa: Optional[list[str]] = FieldDescriptor[list[str]](input_name="AAAA", required=False)
    cname: Optional[CNAMERecord] = FieldDescriptor[CNAMERecord](input_name="CNAME", required=False)
    mx: Optional[MXRecord] = FieldDescriptor[MXRecord](input_name="MX", required=False)
    ns: Optional[list[NSRecord]] = FieldDescriptor[list[NSRecord]](input_name="NS", required=False)
    txt: Optional[list[str]] = FieldDescriptor[list[str]](input_name="TXT", required=False)


class TLSCertificateExtension(Record):
    critical: bool
    name: str
    value: str


class TLSCertificate(Record):
    common_name: str = StringField(input_name="commonName")
    country: Optional[str] = StringField(input_name="country", required=False)
    is_root: bool = BooleanField(input_name="isRoot")
    organization: Optional[str] = StringField(input_name="organization", required=False)
    valid_len: int = IntegerField(input_name="validLen")
    validity_end: float = FloatField(input_name="validityEnd")
    validity_start: float = FloatField(input_name="validityStart")
    extension_count: int = IntegerField(input_name="extensionCount")
    extensions: Optional[list[TLSCertificateExtension]] = FieldDescriptor[list[TLSCertificateExtension]](required=False)


class TLSData(Record):
    protocol: str
    cipher: str
    count: int
    certificates: list[TLSCertificate]


class IPFromRecord(Record):
    ip: str = StringField()
    type: str = StringField()


class DNSResult(Result):
    dns_data: Optional[DNSData] = FieldDescriptor[DNSData](input_name="dnsData", required=False)
    tls_data: Optional[TLSData] = FieldDescriptor[TLSData](input_name="tlsData", required=False)
    # A set of IP addresses coupled with the name of the DNS record type the address was extracted from.
    ips: Optional[set[IPFromRecord]] = FieldDescriptor[set[IPFromRecord]](required=False)
    # A map of [IP Address] -> (map of [Collector ID] -> [Result]).
    ip_results: Optional[dict[str, dict[str, IPResult]]] = FieldDescriptor[
        dict[str, dict[str, IPResult]]](field="ipResults", required=False)


class FinalResult(Record, serializer='json'):
    dns_result: DNSResult = FieldDescriptor[DNSResult](input_name='dnsResult')
    rdap_dn_result: RDAPDomainResult = FieldDescriptor[RDAPDomainResult](input_name='rdapDomainResult')
    # TODO
