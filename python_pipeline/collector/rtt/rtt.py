from json import dumps

from icmplib import async_ping, ICMPSocketError, DestinationUnreachable, TimeExceeded

from collector.util import timestamp_now_millis, should_omit_ip
from common.util import read_config, make_app, serialize_ip_to_process
from common.models import *
import common.result_codes as rc

COLLECTOR = "rtt"

# Read the config
config = read_config()
component_config = config.get(COLLECTOR, {})

# The Faust application
rtt_app = make_app(COLLECTOR, config)

COUNT = component_config.get("ping_count", 5)
PRIVILEGED = component_config.get("privileged_mode", False)
CONCURRENCY = component_config.get("concurrency", 4)

# The input and output topics
topic_to_process = rtt_app.topic('to_process_IP', key_type=IPToProcess,
                                 value_type=IPProcessRequest, allow_empty=True)

# The key is explicitly serialized to avoid Faust injecting its metadata in the output object
topic_processed = rtt_app.topic('collected_IP_data', key_type=bytes,
                                value_type=RTTResult)


# The RDAP-DN processor
@rtt_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    # Main message processing loop
    # dn is the domain name / IP address pair
    async for dn_ip, process_request in stream.items():
        # Omit the DN if the collector is not in the list of collectors to process
        if should_omit_ip(process_request, COLLECTOR):
            continue

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
