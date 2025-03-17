"""rdap_ip.py: The processor for the RDAP-IP collector."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import asyncio
import ipaddress
from typing import Literal

import httpx
import whodap
from whodap import IPv4Client, IPv6Client
from whodap.errors import *
from whodap.response import IPv4Response, IPv6Response

import common.result_codes as rc
from collectors.processor import BaseAsyncCollectorProcessor
from collectors.rdap_util import make_rdap_ssl_context
from common import log
from common.models import IPToProcess, IPProcessRequest, RDAPIPResult
from common.util import dump_model
from domrad_kafka_client import Message, SimpleMessage

COLLECTOR = "rdap-ip"
COMPONENT_NAME = "collector-" + COLLECTOR


class RDAPIPProcessor(BaseAsyncCollectorProcessor[IPToProcess, IPProcessRequest]):
    def __init__(self, config: dict):
        super().__init__(config, COMPONENT_NAME, 'collected_IP_data', IPToProcess, IPProcessRequest, RDAPIPResult)
        self._logger = log.init("worker")

        component_config = config.get(COLLECTOR, {})
        self._httpx_client = httpx.AsyncClient(verify=make_rdap_ssl_context(), follow_redirects=True,
                                               timeout=component_config.get("http_timeout", 5))
        self._ipv4_client = None
        self._ipv6_client = None

    def get_rl_bucket_key(self, message: Message[IPToProcess, IPProcessRequest]) -> str | Literal['default'] | None:
        ip = ipaddress.ip_address(message.key.ip)
        # noinspection PyProtectedMember
        rdap_target = self._ipv4_client._get_rdap_server(ip) if ip.version == 4 \
            else self._ipv6_client._get_rdap_server(ip)
        return rdap_target

    async def init_async(self):
        while True:
            try:
                self._ipv4_client = await whodap.IPv4Client.new_aio_client(httpx_client=self._httpx_client)
                self._ipv6_client = await whodap.IPv6Client.new_aio_client(httpx_client=self._httpx_client)
                break
            except Exception as e:
                self._logger.error("Error initializing RDAP clients. Retrying in 5 seconds.", exc_info=e)
                await asyncio.sleep(5)

    async def close_async(self):
        await self._ipv4_client.aio_close()
        await self._ipv6_client.aio_close()
        await self._httpx_client.aclose()

    async def process(self, message: Message[IPToProcess, IPProcessRequest]) -> list[SimpleMessage]:
        logger = self._logger
        dn_ip = message.key
        process_request = message.value

        if dn_ip is None:
            return []

        # Omit the DN if the collector is not in the list of collectors to process
        if self._should_omit_ip(dn_ip, process_request):
            return []

        logger.k_trace("Processing %s", dn_ip.domain_name, dn_ip.ip)
        rdap_data, err_code, err_msg = await self.fetch_ip(dn_ip.domain_name, dn_ip.ip)
        if rdap_data is not None:
            rdap_data = rdap_data.to_dict()

        result = RDAPIPResult(status_code=err_code, error=err_msg,
                              collector=COLLECTOR,
                              data=rdap_data)

        return [(self._output_topic, message.key_raw, dump_model(result))]

    async def fetch_ip(self, dn, address) \
            -> tuple[IPv4Response | IPv6Response | None, int | None, str | None]:
        client_v4: IPv4Client = self._ipv4_client
        client_v6: IPv6Client = self._ipv6_client
        logger = self._logger

        ip = ipaddress.ip_address(address)
        # noinspection PyProtectedMember
        rdap_target = client_v4._get_rdap_server(ip) if ip.version == 4 else client_v6._get_rdap_server(ip)

        try:
            if ip.version == 4:
                logger.k_trace("Fetching for IPv4: %s / %s", dn, address, rdap_target)
                ip_data = await client_v4.aio_lookup(address)
            else:
                logger.k_trace("Fetching for IPv6: %s / %s", dn, address, rdap_target)
                ip_data = await client_v6.aio_lookup(address)
            return ip_data, 0, None
        except httpx.TimeoutException:
            logger.k_debug("HTTP timeout", dn)
            return None, rc.TIMEOUT, None
        except (httpx.NetworkError, IOError) as e:
            logger.k_warning("Network error", dn, e=e)
            return None, rc.CANNOT_FETCH, "Network error: " + str(e)
        except BadStatusCode as e:
            logger.k_debug("RDAP weird status code - %s", str(e))
            return None, rc.CANNOT_FETCH, str(e)
        except MalformedQueryError as e:
            logger.k_warning("Malformed query: %s", dn, address, e=e)
            return None, rc.INTERNAL_ERROR, str(e)
        except NotFoundError:
            logger.k_debug("Not found: %s", dn, address)
            return None, rc.NOT_FOUND, None
        except RateLimitError as e:
            # TODO: Implement and use explicit RL bucket filling mechanism
            logger.k_info("Remote rate limited: %s / %s: %s", dn, address, rdap_target, str(e))
            return None, rc.RATE_LIMITED, str(e)
        except WhodapError as e:
            logger.k_warning("General whodap error: %s", dn, address, e=e)
            return None, rc.CANNOT_FETCH, str(e)
        except ValueError as e:
            logger.k_debug("Invalid address: %s", dn, address)
            return None, rc.INVALID_ADDRESS, str(e)
        except Exception as e:
            logger.k_warning("Unhandled exception: %s", dn, address, e=e)
            return None, rc.INTERNAL_ERROR, str(e)
