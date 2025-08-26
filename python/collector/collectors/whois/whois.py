"""whois.py: The processor for the WHOIS collector."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import socket
from typing import Literal

import tldextract

import common.result_codes as rc
from collectors.processor import BaseAsyncCollectorProcessor
from common import log
from common.models import WHOISRequest, WHOISResult
from common.util import dump_model
from domrad_kafka_client import Message, SimpleMessage

from .client import WhoisClient, find_error
from .client.iana_db import IanaDBGenerator
from ..rdap_util import extract_known_tld

COLLECTOR = "whois"
COMPONENT_NAME = "collector-" + COLLECTOR


class WHOISProcessor(BaseAsyncCollectorProcessor[str, WHOISRequest]):
    def __init__(self, config: dict):
        super().__init__(config, COMPONENT_NAME, 'processed_WHOIS', str, WHOISRequest, WHOISResult)
        self._logger = log.init("worker")

        component_config = config.get(COLLECTOR, {})

        self._endpoint_provider = IanaDBGenerator(component_config.get("iana_db_path"),
                                                  component_config.get("iana_db_lifetime"))
        self._logger.info("Loading IANA WHOIS DB")

        db = self._endpoint_provider.get_db()  # Preload the DB
        if len(db) < 2:
            self._logger.error("IANA WHOIS DB is empty, cannot fetch WHOIS! Halting.")
            raise RuntimeError("IANA WHOIS DB is empty")

        self._logger.info("Loaded (%s entries)", len(db) - 1)

        self._timeout = component_config.get("timeout", 5.0)

        self._whois_client = WhoisClient()
        self._rate_limiters = config.get("rate_limiter", {})

    def deserialize(self, message: Message[str, WHOISRequest]) -> None:
        super().deserialize(message)
        # Store a pre-extracted TLD and WHOIS endpoint in the message for later use
        db = self._endpoint_provider.get_db()
        _, tld, endpoint = extract_known_tld(message.key, db)
        message.custom_data = (tld, endpoint)

    def get_rl_bucket_key(self, message: Message[str, WHOISRequest]) -> str | Literal['default'] | None:
        # Rate limit by endpoint (if such rate limiter definition exists), otherwise by TLD
        tld, endpoint = message.custom_data
        if endpoint in self._rate_limiters:
            return endpoint
        return tld

    async def process(self, message: Message[str, WHOISRequest]) -> list[SimpleMessage]:
        logger = self._logger

        # Main message processing loop
        dn = message.key
        req = message.value
        logger.k_trace("Processing WHOIS", dn)
        _, target_endpoint = message.custom_data

        # Extract the zone DN if present in the request
        if req is not None and req.zone is not None:
            zone = req.zone
        else:
            zone = None

        if zone is not None:
            # If the zone DN is available, get WHOIS data for it. There's no point in trying the actual
            # source domain name, WHOIS provide the same data for points of delegation
            target = zone
        else:
            # If the zone DN is not available, try to get WHOIS data for the actual source domain name
            target = dn

        whois_raw, whois_err_code, whois_err_msg = await self.fetch_whois(target, target_endpoint)
        logger.k_trace("Got result %s for target %s", dn, whois_err_code, target)

        if whois_err_code == rc.NOT_FOUND:
            # If the WHOIS data is not found for the source DN, try to get it for the registered domain name
            # (i.e. one level above the public suffix)
            target_parts = tldextract.extract(target)
            if target_parts.domain != "" and target_parts.suffix != "":
                reg_whois_target = target_parts.domain + "." + target_parts.suffix
                if reg_whois_target != target:
                    target = reg_whois_target
                    logger.k_trace("Retrying on target %s", dn, target)
                    whois_raw, whois_err_code, whois_err_msg = await self.fetch_whois(target, target_endpoint)

        result = WHOISResult(status_code=whois_err_code, error=whois_err_msg,
                             whois_target=target, raw_response=whois_raw)

        logger.k_trace("Sending WHOIS result", dn)
        return [(self._output_topic, message.key_raw, dump_model(result))]

    async def fetch_whois(self, domain_name: str, target_endpoint: str | None) -> tuple[
        str | None, int | None, str | None]:
        client = self._whois_client
        logger = self._logger

        if target_endpoint is None:
            logger.k_debug("No WHOIS server known", domain_name)
            return None, rc.NO_ENDPOINT, None

        try:
            whois_raw = await client.query_async(query=domain_name, server=target_endpoint, timeout=self._timeout)
            if (err_code := find_error(whois_raw)) is not None:
                # Produce the NOT_FOUND or RATE_LIMITED error code if we can find it in the response
                return whois_raw, err_code, None
            return whois_raw, rc.OK, None
        except (TimeoutError, socket.timeout) as e:
            logger.k_debug("WHOIS timeout", domain_name)
            return None, rc.TIMEOUT, str(e)
        except (ConnectionResetError, OSError) as e:
            logger.k_debug("WHOIS socket error", domain_name, e=e)
            return None, rc.CANNOT_FETCH, str(e)
        except Exception as e:
            logger.k_warning("Unhandled WHOIS exception", domain_name, e=e)
            return None, rc.INTERNAL_ERROR, str(e)
