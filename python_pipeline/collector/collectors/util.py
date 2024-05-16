import ssl
from datetime import datetime
from typing import List, Optional

from faust.models import FieldDescriptor
from whodap import DNSClient
from whodap.response import DomainResponse, RDAPResponse
from whodap.client import RDAPClient

from common.audit import log_unhandled_error
from common.models import IPProcessRequest, Result
from common import result_codes as rc


async def fetch_entities(response: DomainResponse, client: RDAPClient) -> List[RDAPResponse]:
    if not hasattr(response, "entities"):
        return []

    result = []
    for entity in response.entities:
        if hasattr(entity, "links") and not hasattr(entity, "vcardArray"):
            for link in entity.links:
                if link.rel != "self":
                    continue

                entity_response = None
                try:
                    entity_response = await client._aio_get_authoritative_response(link.href, [link.href])
                finally:
                    if entity_response is None:
                        # If the entity could not be fetched, add the original entity
                        result.append(entity)
                        break

                entity_response_json = entity_response.read()
                fetched_entity = RDAPResponse.from_json(entity_response_json)
                # Add the original "roles" array from the source entity to the entity fetched from the link
                if hasattr(entity, "roles"):
                    fetched_entity.roles = entity.roles
                result.append(fetched_entity)
                break
        else:
            result.append(entity)

    del response.entities
    return result


def make_rdap_ssl_context():
    context = ssl.create_default_context()
    context.set_ciphers("ALL:@SECLEVEL=1")
    return context


def timestamp_now_millis():
    return int(datetime.now().timestamp() * 1e3)


def extract_known_tld(domain_name: str, client: DNSClient) -> tuple[str | None, str | None, str | None]:
    """
    Extract a TLD (or another suffix) registered in the IANA DNS RDAP bootstrap file.
    The bootstrap data are taken from the given DNSClient instance.

    :return: A tuple with the domain, the suffix part and the RDAP server base URL.
    All are None if the TLD is not found in the IANA's registry.
    """
    parts = domain_name.split('.')
    if len(parts) < 2:
        return None, None, None

    domain = '.'.join(parts[:-2])
    # Try the SLD first
    tld = '.'.join(parts[-2:])

    iana_tlds = client.iana_dns_server_map
    if tld not in iana_tlds:
        # SLD not found in IANA's registry, try the TLD
        domain = '.'.join(parts[:-1])
        tld = parts[-1]

    if tld not in iana_tlds:
        # TLD not found in IANA's registry
        return None, None, None

    # Return the domain and the suffix part
    return domain, tld, iana_tlds.get(tld)


def should_omit_ip(request: Optional[IPProcessRequest], collector_name: str) -> bool:
    return request is not None and request.collectors is not None and collector_name not in request.collectors


def get_ip_safe(dn_ip):
    return str(dn_ip.ip) if dn_ip is not None and hasattr(dn_ip, "ip") else None


async def handle_top_level_component_exception(exc_info, component_id, key, result_class, topic):
    def make_error_result_instance(error):
        # noinspection PyBroadException
        try:
            attributes = result_class.__dict__
            kwargs = {
                "status_code": rc.INTERNAL_ERROR,
                "error": error,
                "last_attempt": timestamp_now_millis()
            }

            for attr_name in result_class.__dict__:
                if attr_name.startswith("_") or attr_name in kwargs:
                    continue
                attr = attributes[attr_name]
                if not isinstance(attr, FieldDescriptor):
                    continue
                if attr_name == "collector":
                    kwargs[attr_name] = component_id
                    continue
                try:
                    kwargs[attr_name] = attr.default if not attr.required else attr.type()
                except TypeError:
                    kwargs[attr_name] = None

            return result_class(**kwargs)
        except Exception:
            return Result(status_code=rc.INTERNAL_ERROR, error=error, last_attempt=timestamp_now_millis())

    try:
        result = make_error_result_instance(str(exc_info))
        await topic.send(key=key, value=result)
    except Exception as exc_info:
        log_unhandled_error(exc_info, component_id, key, desc="Error sending the error message to the output topic")
