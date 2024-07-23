"""rdap_util.py: Utility functions for the RDAP collectors."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import ssl
from typing import List

from whodap import DomainResponse, DNSClient
from whodap.client import RDAPClient
from whodap.response import RDAPResponse


async def fetch_entities(response: DomainResponse, client: RDAPClient) -> List[RDAPResponse]:
    """
    Asynchronously fetches RDAP entities related to a given domain response.

    This function takes a DomainResponse object and an RDAPClient object as input. It checks if the response has
    entities. If not, it returns an empty list.

    For each entity in the response, it checks if the entity has links and does not have a vcardArray. If so, it
    iterates over the links. It then tries to get a RDAP response for the link's href. If it fails, it adds the original
    entity to the result and breaks the loop.

    If it succeeds, it reads the response, creates an RDAPResponse object from the response JSON, and adds the original
    entity's roles to the fetched entity if they exist. It then adds the fetched entity to the result. If the entity
    does not have links or has a vcardArray, it adds the entity to the result. Finally, it deletes the entities from the
    original response and returns the result.

    Args:
        response (DomainResponse): The domain response to fetch entities for.
        client (RDAPClient): The RDAP client to use for fetching entities.

    Returns:
        List[RDAPResponse]: A list of RDAPResponse objects representing the fetched entities.
    """
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
    """Creates an SSL context with a lower security level for RDAP requests."""
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
