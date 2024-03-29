import ssl
from datetime import datetime
from typing import List

from whodap import DNSClient
from whodap.response import DomainResponse, RDAPResponse
from whodap.client import RDAPClient


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


def make_ssl_context():
    context = ssl.create_default_context()
    context.set_ciphers("ALL:@SECLEVEL=1")
    return context


def timestamp_now_millis():
    return int(datetime.now().timestamp() * 1e3)


def extract_known_tld(domain_name: str, client: DNSClient) -> tuple[str | None, str | None]:
    """
    Extract a TLD (or another suffix) registered in the IANA DNS RDAP bootstrap file.
    The bootstrap data are taken from the given DNSClient instance.

    :return: A tuple with the domain and the suffix part. Both are None if the TLD is not found in the IANA's registry.
    """
    parts = domain_name.split('.')
    if len(parts) < 2:
        return None, None

    domain = '.'.join(parts[:-1])
    # Try the SLD first
    tld = '.'.join(parts[-2:])

    iana_tlds = client.iana_dns_server_map.keys()
    if tld not in iana_tlds:
        # SLD not found in IANA's registry, try the TLD
        tld = parts[-1]

    if tld not in iana_tlds:
        # TLD not found in IANA's registry
        return None, None

    # Return the domain and the suffix part
    return domain, parts[-1]
