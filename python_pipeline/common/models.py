from typing import Optional

from faust import Record
from faust.models.fields import FieldDescriptor, IntegerField, StringField, BooleanField, FloatField


class IPToProcess(Record, coerce=True, serializer='json'):
    domain_name: str = StringField(field="domainName", required=True)
    ip: str = StringField(field="ip", required=True)


class IPProcessRequest(Record, coerce=True, serializer='json'):
    collectors: Optional[list[str]] = FieldDescriptor[list[str]](field="collectors", required=False)


class Result(Record, abstract=False, coerce=True, serializer='json'):
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
    rdap_target: bool = StringField(field="rdapTarget", required=True)

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


class ZoneRequest(Record, coerce=True, serializer='json'):
    collect_dns: bool = BooleanField(field="collectDNS", required=False, default=True)
    collect_RDAP: bool = BooleanField(field="collectRDAP", required=False, default=True)
    dns_types_to_collect: Optional[list[str]] = FieldDescriptor[list[str]](field="dnsTypesToCollect", required=False)
    dns_types_to_process_IPs_from: Optional[list[str]] = FieldDescriptor[list[str]](field="dnsTypesToProcessIPsFrom",
                                                                                    required=False)


class ZoneInfo(Record):
    zone: str = StringField(required=True)
    soa: SOARecord = FieldDescriptor[SOARecord](required=True)
    public_suffix: str = StringField(field="publicSuffix", required=True)
    registry_suffix: str = StringField(field="registrySuffix", required=True)
    primary_nameserver_ips: Optional[set[str]] = FieldDescriptor[set[str]](field="primaryNameserverIps",
                                                                           required=False)
    secondary_nameservers: Optional[set[str]] = FieldDescriptor[set[str]](field="secondaryNameservers",
                                                                          required=False)
    secondary_nameserver_ips: Optional[set[str]] = FieldDescriptor[set[str]](field="secondaryNameserverIps",
                                                                             required=False)


class ZoneResult(Result, abstract=False):
    zone: ZoneInfo = FieldDescriptor[ZoneInfo](required=False)


class DNSRequest(Record, coerce=True, serializer='json'):
    dns_types_to_collect: Optional[list[str]] = FieldDescriptor[list[str]](field="dnsTypesToCollect",
                                                                           required=False)
    dns_types_to_process_IPs_from: Optional[list[str]] = FieldDescriptor[list[str]](
        field="dnsTypesToProcessIPsFrom",
        required=False)

    zone_info: ZoneInfo = FieldDescriptor[ZoneInfo](field="zoneInfo", required=True)
