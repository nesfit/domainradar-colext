"""rdap_dn.py: The processor for the RDAP-DN collector."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import asyncio
from typing import Literal

import asyncwhois
import httpx
import tldextract
import whodap
from asyncwhois.errors import *
from asyncwhois.query import DomainQuery
from whodap.errors import *
from whodap.response import DomainResponse

import common.result_codes as rc
from collectors.processor import BaseAsyncCollectorProcessor
from collectors.rdap_util import fetch_entities, make_rdap_ssl_context, extract_known_tld
from common import log
from common.models import RDAPDomainRequest, RDAPDomainResult
from common.util import dump_model
from domrad_kafka_client import Message, SimpleMessage

COLLECTOR = "rdap-dn"
COMPONENT_NAME = "collector-" + COLLECTOR


class RDAPDNProcessor(BaseAsyncCollectorProcessor[str, RDAPDomainRequest]):
    def __init__(self, config: dict):
        super().__init__(config, COMPONENT_NAME, 'processed_RDAP_DN', str, RDAPDomainRequest, RDAPDomainResult)
        self._logger = log.init("worker")

        component_config = config.get(COLLECTOR, {})
        self._httpx_client = httpx.AsyncClient(verify=make_rdap_ssl_context(), follow_redirects=True,
                                               timeout=component_config.get("http_timeout", 5))
        self._whois_client = asyncwhois.client.DomainClient()
        self._rdap_client = None

        self._rate_limiters = config.get("rate_limiter", {})

    # def deserialize(self, message: Message[str, RDAPDomainRequest]):
    #     super().deserialize(message)
    #     # TODO: pre-process TLD?

    def get_rl_bucket_key(self, message: Message[str, RDAPDomainRequest]) -> str | Literal['default'] | None:
        _, tld, endpoint = extract_known_tld(message.key, self._rdap_client)
        if endpoint in self._rate_limiters:
            return endpoint
        return tld

    async def init_async(self):
        while True:
            try:
                self._rdap_client = await whodap.DNSClient.new_aio_client(httpx_client=self._httpx_client)
                break
            except Exception as e:
                self._logger.error("Error initializing RDAP client. Retrying in 5 seconds.", exc_info=e)
                await asyncio.sleep(5)

    async def close_async(self):
        await self._rdap_client.aio_close()
        await self._httpx_client.aclose()

    async def process(self, message: Message[str, RDAPDomainRequest]) -> list[SimpleMessage]:
        logger = self._logger

        # Main message processing loop
        dn = message.key
        req = message.value
        logger.k_trace("Processing RDAP", dn)

        # The default WHOIS results are empty and with a status code signalising that WHOIS was not used
        whois_raw, whois_parsed, whois_err_code, whois_err_msg = None, None, rc.WHOIS_NOT_PERFORMED, None
        zone = None

        # Extract the zone DN if present in the request
        if req is not None and req.zone is not None:
            zone = req.zone

        if zone is not None:
            # If the zone DN is available, get RDAP data for it. There's no point in trying the actual
            # source domain name, RDAP should only provide data for points of delegation
            rdap_target = zone
        else:
            # If the zone DN is not available, try to get RDAP data for the actual source domain name
            rdap_target = dn

        rdap_data, entities, err_code, err_msg = await self.fetch_rdap(rdap_target)
        logger.k_trace("Got result %s for target %s", dn, err_code, rdap_target)

        if rdap_data is None:
            # If the RDAP data is not available for the source DN, try to get it for the registered domain name
            # (i.e. one level above the public suffix)
            target_parts = tldextract.extract(rdap_target)
            if target_parts.domain != "" and target_parts.suffix != "":
                reg_rdap_target = target_parts.domain + "." + target_parts.suffix
                if reg_rdap_target != rdap_target:
                    rdap_target = reg_rdap_target
                    logger.k_trace("Retrying on target %s", dn, rdap_target)
                    rdap_data, entities, err_code, err_msg = await self.fetch_rdap(rdap_target)

        if rdap_data is None:
            # Only try WHOIS if no RDAP data is available at any level of the source DN
            logger.k_debug("No RDAP, trying WHOIS for %s", dn, zone or dn)
            (whois_raw, whois_parsed,
             whois_err_code, whois_err_msg) = await self.fetch_whois(zone or dn)
        else:
            logger.k_trace("Got RDAP data for %s", dn, rdap_target)
            # Convert the SimpleNamespace objects to JSON-serializable dictionaries
            rdap_data = rdap_data.to_dict()
            entities = [e.to_dict() for e in entities]

        result = RDAPDomainResult(status_code=err_code, error=err_msg,
                                  rdap_data=rdap_data, entities=entities,
                                  rdap_target=rdap_target,
                                  whois_status_code=whois_err_code, whois_error=whois_err_msg,
                                  raw_whois_data=whois_raw, parsed_whois_data=whois_parsed)

        logger.k_trace("Sending RDAP result", dn)
        return [(self._output_topic, message.key_raw, dump_model(result))]

    async def fetch_rdap(self, domain_name) \
            -> tuple[DomainResponse | None, list | None, int | None, str | None]:
        client = self._rdap_client
        logger = self._logger

        domain, tld, rdap_base = extract_known_tld(domain_name, client)
        if domain is None or tld is None:
            return None, None, rc.NO_ENDPOINT, None

        try:
            logger.k_trace("Fetching RDAP: %s", domain_name, rdap_base)
            rdap_data = await client.aio_lookup(domain, tld)
            entities = await fetch_entities(rdap_data, client)
            return rdap_data, entities, 0, None
        except httpx.TimeoutException:
            logger.k_debug("HTTP timeout", domain_name)
            return None, None, rc.TIMEOUT, None
        except (httpx.NetworkError, IOError) as e:
            logger.k_warning("Network error", domain_name, e=e)
            return None, None, rc.CANNOT_FETCH, "Network error: " + str(e)
        except BadStatusCode as e:
            logger.k_debug("RDAP weird status code - %s", str(e))
            return None, None, rc.CANNOT_FETCH, str(e)
        except MalformedQueryError as e:
            logger.k_warning("Malformed query", domain_name, e=e)
            return None, None, rc.INTERNAL_ERROR, str(e)
        except NotFoundError:
            logger.k_debug("Not found", domain_name)
            return None, None, rc.NOT_FOUND, None
        except RateLimitError as e:
            # TODO: Implement and use explicit RL bucket filling mechanism
            logger.k_debug("Remote rate limited: %s: %s", domain_name, rdap_base, str(e))
            return None, None, rc.RATE_LIMITED, str(e)
        except WhodapError as e:
            logger.k_warning("General whodap error", domain_name, e=e)
            return None, None, rc.CANNOT_FETCH, str(e)
        except Exception as e:
            logger.k_warning("Unhandled exception", domain_name, e=e)
            return None, None, rc.INTERNAL_ERROR, str(e)

    async def fetch_whois(self, domain_name) -> tuple[
        str | None, dict | None, int | None, str | None]:
        client = self._whois_client
        logger = self._logger

        suffix = tldextract.extract(domain_name).suffix
        suffix = suffix.split(".")[-1]
        # noinspection PyProtectedMember
        endpoint = DomainQuery._get_server_name(suffix) or DomainQuery.iana_server

        try:
            whois_raw, whois_parsed = await client.aio_whois(domain_name)

            if whois_parsed is not None:
                whois_parsed = {k: v for k, v in whois_parsed.items() if v is not None
                                and (not isinstance(v, list) or len(v) > 0)}

            return whois_raw, whois_parsed, 0, None
        except NotFoundError as e:
            logger.k_debug("WHOIS not found", domain_name)
            return None, None, rc.NOT_FOUND, str(e)
        except WhoIsError as e:
            msg = str(e).lower()
            if "rate" in msg or "limit" in msg:
                logger.k_info("WHOIS remote rate limited: %s: %s", domain_name, endpoint, msg)
                # TODO: Implement and use explicit RL bucket filling mechanism
                return None, None, rc.RATE_LIMITED, str(e)
            elif "domain not found" in msg:
                logger.k_debug("WHOIS not found", domain_name)
                return None, None, rc.NOT_FOUND, str(e)

            logger.k_debug("WHOIS other error: %s: %s", domain_name, endpoint, msg)
            return None, None, rc.CANNOT_FETCH, str(e)
        except ConnectionResetError as e:
            logger.k_debug("WHOIS connection reset error", domain_name)
            return None, None, rc.CANNOT_FETCH, str(e)
        except TimeoutError as e:
            logger.k_debug("WHOIS timeout", domain_name)
            return None, None, rc.TIMEOUT, str(e)
        except Exception as e:
            logger.k_warning("Unhandled WHOIS exception", domain_name, e=e)
            if isinstance(e, IOError):
                return None, None, rc.INTERNAL_ERROR, str(e)
            else:
                return None, None, rc.CANNOT_FETCH, str(e)
