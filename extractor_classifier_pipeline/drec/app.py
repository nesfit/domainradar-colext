import faust
from models import *

# Define the Faust application with a unique name and the Kafka broker URL
app = faust.App('drce-extractor',
                broker='kafka://localhost:9092',
                debug=True)


# Define a simple data model for the entries
class Entry(faust.Record, serializer='json'):
    data: str


# Define the input and output topics
topic_input = app.topic('all_collected_data', value_type=FinalResult)


# topic_output = app.topic('processed', value_type=str)

# Define the Faust agent that consumes from the input topic, processes each entry, and sends to the output topic
@app.agent(topic_input)
async def process_stream(stream):
    async for entry in stream:
        result: FinalResult = entry
        print(result.dumps())


if __name__ == '__main__':
    app.main()
