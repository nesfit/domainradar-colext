"""rtt.py: The Faust application for the RTT collector."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from icmplib import async_ping, ICMPSocketError, DestinationUnreachable, TimeExceeded

import common.result_codes as rc
from collectors.util import should_omit_ip, handle_top_level_exception
from common import read_config, make_app, log
from common.models import IPToProcess, IPProcessRequest, RTTResult, RTTData
from common.util import ensure_model

COLLECTOR = "rtt"
COMPONENT_NAME = "collector-" + COLLECTOR

# Read the config
config = read_config()
component_config = config.get(COLLECTOR, {})
logger = log.init(COMPONENT_NAME, config)

COUNT = component_config.get("ping_count", 5)
PRIVILEGED = component_config.get("privileged_mode", False)
CONCURRENCY = component_config.get("concurrency", 4)

# The Faust application
rtt_app = make_app(COLLECTOR, config)

# The input and output topics
topic_to_process = rtt_app.topic('to_process_IP', allow_empty=True)
topic_processed = rtt_app.topic('collected_IP_data')


async def process_entry(dn_ip):
    rtt_data = None
    try:
        ping_result = await async_ping(dn_ip.ip, count=COUNT, privileged=PRIVILEGED)
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

    # (this could probably be send_soon not to block the loop)
    await topic_processed.send(key=dn_ip,
                               value=RTTResult(status_code=code, error=err_msg, collector=COLLECTOR, data=rtt_data))


# The RTT processor
@rtt_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    # Main message processing loop
    # dn is the domain name / IP address pair
    async for dn_ip, process_request in stream.items():
        dn_ip = ensure_model(IPToProcess, dn_ip)
        process_request = ensure_model(IPProcessRequest, process_request)

        if dn_ip is None:
            continue

        try:
            # Omit the DN if the collector is not in the list of collectors to process
            if should_omit_ip(process_request, COLLECTOR):
                logger.k_trace("Omitting IP %s", dn_ip.domain_name, dn_ip.ip)
                continue

            logger.k_trace("Processing %s", dn_ip.domain_name, dn_ip.ip)
            await process_entry(dn_ip)
        except Exception as e:
            logger.k_unhandled_error(e, str(dn_ip))
            await handle_top_level_exception(e, COMPONENT_NAME, dn_ip, RTTResult, topic_processed)
