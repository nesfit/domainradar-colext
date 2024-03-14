from typing import Optional

from faust import Record
from faust.models.fields import FieldDescriptor, FloatField, IntegerField, StringField, BooleanField


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
    soa: Optional[SOARecord] = FieldDescriptor[SOARecord](input_name="SOA", required=False)
    # FIXME
    ns: Optional[NSRecord] = FieldDescriptor[NSRecord](input_name="NS", required=False)
    a: Optional[list[str]] = FieldDescriptor[list[str]](input_name="A", required=False)
    aaaa: Optional[list[str]] = FieldDescriptor[list[str]](input_name="AAAA", required=False)
    cname: Optional[CNAMERecord] = FieldDescriptor[CNAMERecord](input_name="CNAME", required=False)
    mx: Optional[MXRecord] = FieldDescriptor[MXRecord](input_name="MX", required=False)
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


class Result(Record, abstract=True, coerce=True):
    success: bool = BooleanField(required=True)
    error: Optional[str] = StringField(required=False)
    last_attempt: float = FloatField(input_name="lastAttempt")


class IPResult(Result):
    collector: str
    data: dict[str, any]


class DNSResult(Result):
    dns_data: Optional[DNSData] = FieldDescriptor[DNSData](input_name="dnsData", required=False)
    tls_data: Optional[TLSData] = FieldDescriptor[TLSData](input_name="tlsData", required=False)
    ips: Optional[dict[str, dict[str, IPResult]]] = FieldDescriptor[dict[str, dict[str, IPResult]]](required=False)


class RDAPDomainResult(Result):
    # TODO
    reg_date: str = StringField(input_name="registrationDate")
    last_change_date = StringField(input_name="lastChangedDate")


class FinalResult(Record, serializer='json'):
    dns_result: DNSResult = FieldDescriptor[DNSResult](input_name='dnsResult')
    rdap_dn_result: RDAPDomainResult = FieldDescriptor[RDAPDomainResult](input_name='rdapDomainResult')
