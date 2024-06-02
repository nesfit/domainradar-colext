import datetime
from typing import Optional, Any

from pydantic import BaseModel, Field, ConfigDict, AliasGenerator, AliasChoices
from pydantic.alias_generators import to_camel


class CustomBaseModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=AliasGenerator(
            validation_alias=lambda field: AliasChoices(field, to_camel(field)),
            serialization_alias=to_camel
        )
    )


# ---- Plain models ---- #

class IPToProcess(CustomBaseModel):
    domain_name: str = Field(serialization_alias="domainName")
    ip: str


class RTTData(CustomBaseModel):
    min: float
    avg: float
    max: float
    sent: int
    received: int
    jitter: float


class SOARecord(CustomBaseModel):
    primary_ns: str = Field(serialization_alias="primaryNs")
    resp_mailbox_dname: str = Field(serialization_alias="respMailboxDname")
    serial: str
    refresh: int
    retry: int
    expire: int
    min_ttl: int = Field(serialization_alias="minTTL")


class ZoneInfo(CustomBaseModel):
    zone: str
    soa: SOARecord
    public_suffix: str = Field(serialization_alias="publicSuffix")
    registry_suffix: str = Field(serialization_alias="registrySuffix")
    primary_nameserver_ips: Optional[list[str]] = Field(None, serialization_alias="primaryNameserverIps")
    secondary_nameservers: Optional[list[str]] = Field(None, serialization_alias="secondaryNameservers")
    secondary_nameserver_ips: Optional[list[str]] = Field(None, serialization_alias="secondaryNameserverIps")


# ---- Requests ---- #

class ZoneRequest(CustomBaseModel):
    collect_dns: bool = Field(True, serialization_alias="collectDNS")
    collect_RDAP: bool = Field(True, serialization_alias="collectRDAP")
    dns_types_to_collect: Optional[list[str]] = Field(None, serialization_alias="dnsTypesToCollect")
    dns_types_to_process_IPs_from: Optional[list[str]] = Field(None,
                                                               serialization_alias="dnsTypesToProcessIPsFrom")


class DNSRequest(CustomBaseModel):
    dns_types_to_collect: Optional[list[str]] = Field(None, serialization_alias="typesToCollect")
    dns_types_to_process_IPs_from: Optional[list[str]] = Field(None,
                                                               serialization_alias="typesToProcessIPsFrom")
    zone_info: ZoneInfo = Field(serialization_alias="zoneInfo")


class RDAPDomainRequest(CustomBaseModel):
    zone: Optional[str] = None


class IPRequest(CustomBaseModel):
    collectors: Optional[list[str]] = None


# ---- Results ---- #

class Result(CustomBaseModel):
    status_code: int = Field(serialization_alias="statusCode")
    error: Optional[str] = None
    last_attempt: datetime.datetime = Field(serialization_alias="lastAttempt",
                                            default_factory=lambda: datetime.datetime.now(datetime.UTC))


class IPResult(Result):
    collector: str = ""


class RDAPIPResult(IPResult):
    data: Optional[dict[str, Any]] = None


class RTTResult(IPResult):
    data: Optional[RTTData] = None


class RDAPDomainResult(Result):
    rdap_target: str = Field("", serialization_alias="rdapTarget")
    whois_status_code: int = Field(-1, serialization_alias="whoisStatusCode")
    rdap_data: Optional[dict[str, Any]] = Field(None, serialization_alias="rdapData")
    entities: Optional[list[dict[str, Any]]] = None
    whois_error: Optional[str] = Field(None, serialization_alias="whoisError")
    raw_whois_data: Optional[str] = Field(None, serialization_alias="whoisRaw")
    parsed_whois_data: Optional[dict[str, Any]] = Field(None, serialization_alias="whoisParsed")


class ZoneResult(Result):
    zone: Optional[ZoneInfo] = None


avro_namespaces = {
    "default": "cz.vut.fit.domainradar.models",
    IPToProcess: "cz.vut.fit.domainradar.models",
    RTTData: "cz.vut.fit.domainradar.models.ip",
    SOARecord: "cz.vut.fit.domainradar.models.dns",
    ZoneInfo: "cz.vut.fit.domainradar.models.dns",
    ZoneRequest: "cz.vut.fit.domainradar.models.requests",
    DNSRequest: "cz.vut.fit.domainradar.models.requests",
    RDAPDomainRequest: "cz.vut.fit.domainradar.models.requests",
    IPRequest: "cz.vut.fit.domainradar.models.requests",
    # IPResult: None,
    # RDAPIPResult: "cz.vut.fit.domainradar.models.results",
    # RTTResult: "cz.vut.fit.domainradar.models.results",
    RDAPDomainResult: "cz.vut.fit.domainradar.models.results",
    ZoneResult: "cz.vut.fit.domainradar.models.results",
}

avro_overrides = {
    IPResult: None,
    RDAPIPResult: "cz.vut.fit.domainradar.models.results.CommonIPResult%[B%",
    RTTResult: "cz.vut.fit.domainradar.models.results.CommonIPResult%cz.vut.fit.domainradar.models.ip.RTTData%"
}
