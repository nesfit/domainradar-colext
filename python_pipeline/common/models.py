from typing import Optional

from pydantic import BaseModel, Field, ConfigDict, AliasGenerator, AliasChoices
from pydantic.alias_generators import to_camel

from common import timestamp_now_millis


class CustomBaseModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=AliasGenerator(
            validation_alias=lambda field: AliasChoices(field, to_camel(field)),
            serialization_alias=to_camel
        )
    )


# ---- Plain models ---- #

class IPToProcess(CustomBaseModel):
    domain_name: str = Field(serialization_alias="dn", validation_alias="dn")
    ip: str

    def __str__(self):
        return f"{self.domain_name}/{self.ip}"


class RTTData(CustomBaseModel):
    min: float
    avg: float
    max: float
    sent: int
    received: int
    jitter: float


class SOARecord(CustomBaseModel):
    primary_ns: str = Field(serialization_alias="primaryNS")
    resp_mailbox_dname: str = Field(serialization_alias="respMailboxDname")
    serial: str
    refresh: int
    retry: int
    expire: int
    min_ttl: int = Field(serialization_alias="minTTL",
                         validation_alias=AliasChoices("min_ttl", "minTTL"))


class ZoneInfo(CustomBaseModel):
    zone: str
    soa: SOARecord
    public_suffix: str = Field(serialization_alias="publicSuffix")
    has_dnskey: Optional[bool] = Field(None, serialization_alias="hasDNSKEY")
    primary_nameserver_ips: set[str] = Field(serialization_alias="primaryNameserverIPs")
    secondary_nameservers: set[str] = Field(serialization_alias="secondaryNameservers")
    secondary_nameserver_ips: set[str] = Field(serialization_alias="secondaryNameserverIPs")


class IPFromRecord(CustomBaseModel):
    ip: str
    rrType: str


class CNAMERecord(CustomBaseModel):
    value: str
    related_ips: Optional[set[str]] = Field(None, serialization_alias="relatedIPs")


class MXRecord(CustomBaseModel):
    value: str
    priority: int
    related_ips: Optional[set[str]] = Field(None, serialization_alias="relatedIPs")


class NSRecord(CustomBaseModel):
    nameserver: str
    related_ips: Optional[set[str]] = Field(None, serialization_alias="relatedIPs")


class DNSData(CustomBaseModel):
    ttl_values: dict[str, int] = Field({}, serialization_alias="ttlValues")
    a: Optional[set[str]] = Field(None, serialization_alias="A")
    aaaa: Optional[set[str]] = Field(None, serialization_alias="AAAA")
    cname: Optional[CNAMERecord] = Field(None, serialization_alias="CNAME")
    mx: Optional[list[MXRecord]] = Field(None, serialization_alias="MX")
    ns: Optional[list[NSRecord]] = Field(None, serialization_alias="NS")
    txt: Optional[list[str]] = Field(None, serialization_alias="TXT")
    errors: Optional[dict[str, str]] = Field(None)


# ---- Requests ---- #

class ZoneRequest(CustomBaseModel):
    collect_dns: bool = Field(True, serialization_alias="collectDNS")
    collect_RDAP: bool = Field(True, serialization_alias="collectRDAP")
    dns_types_to_collect: Optional[list[str]] = Field(None, serialization_alias="dnsTypesToCollect")
    dns_types_to_process_IPs_from: Optional[list[str]] = Field(None,
                                                               serialization_alias="dnsTypesToProcessIPsFrom")


class DNSRequest(CustomBaseModel):
    zone_info: ZoneInfo = Field(serialization_alias="zoneInfo")
    dns_types_to_collect: Optional[list[str]] = Field(None, serialization_alias="typesToCollect")
    dns_types_to_process_IPs_from: Optional[list[str]] = Field(None,
                                                               serialization_alias="typesToProcessIPsFrom")


class RDAPDomainRequest(CustomBaseModel):
    zone: Optional[str] = None


class IPProcessRequest(CustomBaseModel):
    collectors: Optional[list[str]] = None


# ---- Results ---- #

class Result(CustomBaseModel):
    status_code: int = Field(serialization_alias="statusCode")
    error: Optional[str] = None
    last_attempt: int = Field(serialization_alias="lastAttempt", default_factory=lambda: timestamp_now_millis())


class IPResult(Result):
    collector: str = ""


class RDAPIPResult(IPResult):
    data: Optional[dict] = None


class RTTResult(IPResult):
    data: Optional[RTTData] = None


class RDAPDomainResult(Result):
    rdap_target: str = Field("", serialization_alias="rdapTarget")
    whois_status_code: int = Field(-1, serialization_alias="whoisStatusCode")
    rdap_data: Optional[dict] = Field(None, serialization_alias="rdapData")
    entities: Optional[list[dict]] = None
    whois_error: Optional[str] = Field(None, serialization_alias="whoisError")
    raw_whois_data: Optional[str] = Field(None, serialization_alias="whoisRaw")
    parsed_whois_data: Optional[dict] = Field(None, serialization_alias="whoisParsed")


class ZoneResult(Result):
    zone: Optional[ZoneInfo] = None


class DNSResult(Result):
    dns_data: Optional[DNSData] = None
    ips: Optional[list[IPFromRecord]] = None
