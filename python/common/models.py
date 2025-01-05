"""models.py: Pydantic models for the common data structures used in the components."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from typing import Optional

from pydantic import BaseModel, Field, ConfigDict, AliasGenerator
from pydantic.alias_generators import to_camel

from common import timestamp_now_millis
from common import result_codes as rc

class CustomBaseModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=AliasGenerator(
            validation_alias=to_camel,
            serialization_alias=to_camel
        ),
        # Allow to instantiate models with the defined field names instead of aliases
        populate_by_name=True
    )


# ---- Plain models ---- #

class IPToProcess(CustomBaseModel):
    domain_name: str = Field(alias="dn")
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
    primary_ns: str = Field(alias="primaryNS")
    resp_mailbox_dname: str = Field(alias="respMailboxDname")
    serial: str
    refresh: int
    retry: int
    expire: int
    min_ttl: int = Field(alias="minTTL")


class ZoneInfo(CustomBaseModel):
    zone: str
    soa: SOARecord
    public_suffix: str = Field(alias="publicSuffix")
    has_dnskey: Optional[bool] = Field(None, alias="hasDNSKEY")
    primary_nameserver_ips: set[str] = Field(alias="primaryNameserverIPs")
    secondary_nameservers: set[str] = Field(alias="secondaryNameservers")
    secondary_nameserver_ips: set[str] = Field(alias="secondaryNameserverIPs")


class IPFromRecord(CustomBaseModel):
    ip: str
    type: str = Field(alias="rrType")


class CNAMERecord(CustomBaseModel):
    value: str
    related_ips: Optional[set[str]] = Field(None, alias="relatedIPs")


class MXRecord(CustomBaseModel):
    value: str
    priority: int
    related_ips: Optional[set[str]] = Field(None, alias="relatedIPs")


class NSRecord(CustomBaseModel):
    nameserver: str
    related_ips: Optional[set[str]] = Field(None, alias="relatedIPs")


class DNSData(CustomBaseModel):
    ttl_values: dict[str, int] = Field({}, alias="ttlValues")
    a: Optional[set[str]] = Field(None, alias="A")
    aaaa: Optional[set[str]] = Field(None, alias="AAAA")
    cname: Optional[CNAMERecord] = Field(None, alias="CNAME")
    mx: Optional[list[MXRecord]] = Field(None, alias="MX")
    ns: Optional[list[NSRecord]] = Field(None, alias="NS")
    txt: Optional[list[str]] = Field(None, alias="TXT")
    errors: Optional[dict[str, str]] = Field(None)


# ---- Requests ---- #

class ZoneRequest(CustomBaseModel):
    collect_dns: bool = Field(True, alias="collectDNS")
    collect_RDAP: bool = Field(True, alias="collectRDAP")
    dns_types_to_collect: Optional[list[str]] = Field(None, alias="dnsTypesToCollect")
    dns_types_to_process_IPs_from: Optional[list[str]] = Field(None,
                                                               alias="dnsTypesToProcessIPsFrom")


class DNSRequest(CustomBaseModel):
    zone_info: ZoneInfo = Field(alias="zoneInfo")
    dns_types_to_collect: Optional[list[str]] = Field(None, alias="typesToCollect")
    dns_types_to_process_IPs_from: Optional[list[str]] = Field(None,
                                                               alias="typesToProcessIPsFrom")


class RDAPDomainRequest(CustomBaseModel):
    zone: Optional[str] = None


class IPProcessRequest(CustomBaseModel):
    collectors: Optional[list[str]] = None


# ---- Results ---- #

class Result(CustomBaseModel):
    status_code: int = Field(0, alias="statusCode")
    error: Optional[str] = None
    last_attempt: int = Field(alias="lastAttempt", default_factory=lambda: timestamp_now_millis())


class IPResult(Result):
    collector: str = ""


class RDAPIPResult(IPResult):
    data: Optional[dict] = None


class RTTResult(IPResult):
    data: Optional[RTTData] = None


class RDAPDomainResult(Result):
    rdap_target: str = Field("", alias="rdapTarget")
    whois_status_code: int = Field(rc.WHOIS_NOT_PERFORMED, alias="whoisStatusCode")
    rdap_data: Optional[dict] = Field(None, alias="rdapData")
    entities: Optional[list[dict]] = None
    whois_error: Optional[str] = Field(None, alias="whoisError")
    raw_whois_data: Optional[str] = Field(None, alias="whoisRaw")
    parsed_whois_data: Optional[dict] = Field(None, alias="whoisParsed")


class ZoneResult(Result):
    zone: Optional[ZoneInfo] = None


class DNSResult(Result):
    dns_data: Optional[DNSData] = None
    ips: Optional[list[IPFromRecord]] = None
