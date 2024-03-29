import faust
import whodap
import asyncwhois
from models import *

# The RDAP & WHOIS clients
rdap_client = whodap.DNSClient.new_aio_client()
whois_client = asyncwhois.client.DomainClient()

# The Faust application
app = faust.App('drcol-rdap-dn',
                broker='kafka://localhost:9092',
                debug=True)

# The input and output topics
topic_to_process = app.topic('to_process_RDAP_DN', key_type=str, value_type=None)
topic_processed = app.topic('processed_RDAP_DN', key_type=str, value_type=RDAPDomainResult)


# The RDAP-DN processor
@app.agent(topic_to_process)
async def process_entries(stream):
    async for entry in stream:
        # TODO
        ...


if __name__ == '__main__':
    app.main()
