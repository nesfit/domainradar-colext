from typing import Optional

from faust import Record
from faust.models.fields import FieldDescriptor, FloatField, IntegerField, StringField, BooleanField


class Result(Record, abstract=True, coerce=True):
    statusCode: int = IntegerField(required=True)
    error: Optional[str] = StringField(required=False)
    last_attempt: int = IntegerField(input_name="lastAttempt")


class RDAPResult(Result):
    rdap_data: Optional[dict] = FieldDescriptor[dict](input_name="rdapData", required=False)
    entities: Optional[list[dict]] = FieldDescriptor[list[dict]](required=False)


class RDAPDomainResult(RDAPResult):
    raw_whois_data: Optional[str] = StringField(input_name="rawWHOISData", required=False)
    parsed_whois_data: Optional[dict] = FieldDescriptor[dict](input_name="parsedWHOISData", required=False)
    rdap_error: Optional[str] = StringField(input_name="rdapError", required=False)
    is_for_source_name: bool = BooleanField(input_name="forSourceName", required=True)


class RDAPRequest(Record, coerce=True):
    zone: Optional[str] = StringField(required=False)
