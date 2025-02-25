"""rtt.py: The processor for the RTT collector."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from icmplib import async_ping, ICMPSocketError, DestinationUnreachable, TimeExceeded

import common.result_codes as rc
from collectors.util import should_omit_ip, make_top_level_exception_result
from common import log
from common.models import IPToProcess, IPProcessRequest, RTTResult, RTTData
from common.util import ensure_model, dump_model
from domrad_kafka_client import AsyncKafkaMessageProcessor, SimpleMessage

COLLECTOR = "rtt"
COMPONENT_NAME = "collector-" + COLLECTOR


class RTTProcessor(AsyncKafkaMessageProcessor):
    def __init__(self, config: dict):
        super().__init__(config)
        self._logger = log.init("worker")

        component_config = config.get(COLLECTOR, {})
        self._count = component_config.get("ping_count", 5)
        self._privileged = component_config.get("privileged", False)

    async def process(self, key: bytes, value: bytes, partition: int, offset: int) -> list[SimpleMessage]:
        logger = self._logger

        dn_ip = ensure_model(IPToProcess, key)
        process_request = ensure_model(IPProcessRequest, value)

        if dn_ip is None:
            return []

        try:
            # Omit the DN if the collector is not in the list of collectors to process
            if should_omit_ip(process_request, COLLECTOR):
                logger.k_trace("Omitting IP %s", dn_ip.domain_name, dn_ip.ip)
                return []

            logger.k_trace("Processing %s", dn_ip.domain_name, dn_ip.ip)
            result = await self.process_entry(dn_ip)
            return [('collected_IP_data', key, dump_model(result))]
        except Exception as e:
            logger.k_unhandled_error(e, str(dn_ip))
            return [make_top_level_exception_result('collected_IP_data', e, COMPONENT_NAME, dn_ip, RTTResult)]

    async def process_entry(self, dn_ip) -> RTTResult:
        rtt_data = None
        try:
            ping_result = await async_ping(dn_ip.ip, count=self._count, privileged=self._privileged)
            rtt_data = RTTData(min=ping_result.min_rtt, avg=ping_result.avg_rtt, max=ping_result.max_rtt,
                               sent=ping_result.packets_sent, received=ping_result.packets_received,
                               jitter=ping_result.jitter)
            code = 0
            err_msg = None
        except ICMPSocketError as e:
            code = rc.INTERNAL_ERROR
            err_msg = str(e)
        except DestinationUnreachable as e:
            code = rc.ICMP_DEST_UNREACHABLE
            err_msg = str(e)
        except TimeExceeded as e:
            code = rc.ICMP_TIME_EXCEEDED
            err_msg = str(e)
        except Exception as e:
            code = rc.INTERNAL_ERROR
            err_msg = str(e)

        return RTTResult(status_code=code, error=err_msg, collector=COLLECTOR, data=rtt_data)
