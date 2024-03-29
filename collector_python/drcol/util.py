from whodap.response import DomainResponse, RDAPResponse
from whodap.client import RDAPClient


async def fetch_entities(response: DomainResponse, client: RDAPClient):
    if "entities" not in response.__dict__:
        return response

    result = []
    for entity in response.entities:
        if hasattr(entity, "links") and not hasattr(entity, "vcardArray"):
            for link in entity.links:
                if link.rel != "self":
                    continue
                entity_response = await client._aio_get_authoritative_response(link.href, [link.href])
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

    return result