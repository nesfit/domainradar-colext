from typing import Sequence

from faust import EventT
from faust.serializers import codecs

from common.custom_codecs import StringCodec
from common.util import read_config, make_app

from json import loads, dumps, JSONDecodeError

from . import extractor

codecs.register("str", StringCodec())

EXTRACTOR = "extractor"

# Read the config
config = read_config()
component_config = config.get(EXTRACTOR, {})

# Init the list of transformations
extractor.init_transformations(component_config)

# The Faust application
extractor_app = make_app(EXTRACTOR, config)

CONCURRENCY = component_config.get("concurrency", 4)
BATCH_SIZE = component_config.get("batch_size", 50)
BATCH_TIMEOUT = component_config.get("batch_timeout", 5)

# The input and output topics
# Let's deserialize the result into a dict manually
topic_to_process = extractor_app.topic('all_collected_data', key_type=str,
                                       value_type=bytes, allow_empty=True)

topic_processed = extractor_app.topic('feature_vectors', key_type=str,
                                      value_type=bytes)


@extractor_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    def parse_event(event):
        key = event.key  # type: str
        value_bytes = event.value  # type: bytes

        try:
            value = loads(value_bytes)
            value["domain_name"] = key
            value["invalid_data"] = False
            return value
        except JSONDecodeError:
            return {"domain_name": key, "invalid_data": True}

    # Main message processing loop
    async for events_seq in stream.take_events(BATCH_SIZE, within=BATCH_TIMEOUT):  # type: Sequence[EventT]
        events_parsed = (parse_event(e) for e in events_seq)
        feature_vector = extractor.extract_features(events_parsed)
        for result in feature_vector:
            result_bytes = dumps(result, indent=None).encode("utf-8")
            await topic_processed.send(key=result["domain_name"], value=result_bytes)
