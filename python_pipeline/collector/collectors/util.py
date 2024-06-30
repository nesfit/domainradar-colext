import ssl
from typing import List, Optional

from pydantic import ValidationError
from whodap import DNSClient
from whodap.client import RDAPClient
from whodap.response import DomainResponse, RDAPResponse

from common import log
from common import result_codes as rc
from common.models import IPProcessRequest, Result


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
                    # noinspection PyProtectedMember
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
    error_msg = str(exc_info)

    try:
        try:
            result = result_class(status_code=rc.INTERNAL_ERROR, error=error_msg)
        except ValidationError:
            result = Result(status_code=rc.INTERNAL_ERROR, error=error_msg)

        await topic.send(key=key, value=result)
    except Exception as e:
        log.get(component_id).k_unhandled_error(e, key, desc="Error sending the error message to the output topic",
                                                original_error=error_msg, topic=topic)
