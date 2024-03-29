from typing import Optional

from faust import Record
from faust.models.fields import FieldDescriptor, IntegerField, StringField, BooleanField, FloatField
from icmplib import Host


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
