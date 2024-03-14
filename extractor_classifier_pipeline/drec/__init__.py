import faust

# Define the Faust application with a unique name and the Kafka broker URL
app = faust.App('my_simple_processor', broker='kafka://localhost:9092')

# Define a simple data model for the entries
class Entry(faust.Record, serializer='json'):
    data: str

# Define the input and output topics
topic_input = app.topic('processed_entries', value_type=Entry)
topic_output = app.topic('processed', value_type=Entry)

# Define a simple processing function
def process(entry: Entry) -> Entry:
    # Example processing: reverse the string in the data field
    processed_data = entry.data[::-1]
    return Entry(data=processed_data)

# Define the Faust agent that consumes from the input topic, processes each entry, and sends to the output topic
@app.agent(topic_input)
async def process_stream(stream):
    async for entry in stream:
        processed_entry = process(entry)
        # Forward the processed entry to the output topic
        await topic_output.send(value=processed_entry)

if __name__ == '__main__':
    app.main()