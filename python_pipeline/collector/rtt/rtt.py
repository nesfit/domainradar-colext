import faust
from icmplib import async_ping, ICMPSocketError, DestinationUnreachable, TimeExceeded

import common.result_codes as rc
from collector.util import timestamp_now_millis
from common.models import *

COLLECTOR = "icmp"
COUNT = 5
PRIVILEGED = False

# The Faust application
rtt_app = faust.App('drcol-rtt',
                    broker='kafka://localhost:9092',
                    debug=True)

# The input and output topics
topic_to_process = rtt_app.topic('to_process_IP', key_type=IPToProcess, value_type=None, allow_empty=True)

topic_processed = rtt_app.topic('collected_IP_data', key_type=IPToProcess, value_type=RTTResult)


# The RDAP-DN processor
@rtt_app.agent(topic_to_process, concurrency=4)
async def process_entries(stream):
    # Main message processing loop
    # dn is the domain name / IP address pair
    async for dn_ip, _ in stream.items():
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
        await topic_processed.send(key=dn_ip, value=RTTResult(status_code=code, error=err_msg,
                                                              last_attempt=timestamp_now_millis(),
                                                              collector=COLLECTOR,
                                                              data=rtt_data))


if __name__ == '__main__':
    rtt_app.main()
