from icmplib import async_ping, ICMPSocketError, DestinationUnreachable, TimeExceeded

from collectors.util import (timestamp_now_millis, should_omit_ip, get_ip_safe,
                             handle_top_level_component_exception)
from common import read_config, make_app, serialize_ip_to_process
from common.audit import log_unhandled_error
from common.models import IPToProcess, IPProcessRequest, RTTResult, RTTData
import common.result_codes as rc

COLLECTOR = "rtt"

# Read the config
config = read_config()
component_config = config.get(COLLECTOR, {})

COUNT = component_config.get("ping_count", 5)
PRIVILEGED = component_config.get("privileged_mode", False)
CONCURRENCY = component_config.get("concurrency", 4)

# The Faust application
rtt_app = make_app(COLLECTOR, config)

# The input and output topics
topic_to_process = rtt_app.topic('to_process_IP', key_type=IPToProcess,
                                 value_type=IPProcessRequest, allow_empty=True)

# The key is explicitly serialized to avoid Faust injecting its metadata in the output object
topic_processed = rtt_app.topic('collected_IP_data', key_type=bytes,
                                value_type=RTTResult)


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
        code = rc.ICMP_DESTINATION_UNREACHABLE
        err_msg = str(e)
    except TimeExceeded as e:
        code = rc.ICMP_TIME_EXCEEDED
        err_msg = str(e)
    except Exception as e:
        code = rc.INTERNAL_ERROR
        err_msg = str(e)

    # (this could probably be send_soon not to block the loop)
    await topic_processed.send(key=serialize_ip_to_process(dn_ip),
                               value=RTTResult(status_code=code, error=err_msg,
                                               last_attempt=timestamp_now_millis(),
                                               collector=COLLECTOR,
                                               data=rtt_data))


# The RDAP-DN processor
@rtt_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    # Main message processing loop
    # dn is the domain name / IP address pair
    async for dn_ip, process_request in stream.items():
        try:
            # Omit the DN if the collector is not in the list of collectors to process
            if should_omit_ip(process_request, COLLECTOR):
                continue

            await process_entry(dn_ip)
        except Exception as e:
            ip = get_ip_safe(dn_ip)
            log_unhandled_error(e, COLLECTOR, str(ip), dn_ip=dn_ip)
            await handle_top_level_component_exception(e, COLLECTOR, serialize_ip_to_process(dn_ip),
                                                       RTTResult, topic_processed)
