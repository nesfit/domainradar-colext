"""app.py: The main module for the feature extractor component. Defines the Faust application."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import io
from json import loads, JSONDecodeError
from typing import Sequence

import faust.events
from faust import EventT
from faust.serializers import codecs

from common import read_config, make_app, StringCodec
from common.audit import log_unhandled_error
from . import extractor

codecs.register("str", StringCodec())

EXTRACTOR = "extractor"

# Read the config
config = read_config()
component_config = config.get(EXTRACTOR, {})

CONCURRENCY = component_config.get("concurrency", 4)
BATCH_SIZE = component_config.get("batch_size", 50)
BATCH_TIMEOUT = component_config.get("batch_timeout", 5)

# Init the list of transformations
extractor.init_transformations(component_config)

# The Faust application
extractor_app = make_app(EXTRACTOR, config)

# The input and output topics
# Let's deserialize the result into a dict manually
topic_to_process = extractor_app.topic('all_collected_data', key_type=str,
                                       value_type=bytes, value_serializer='raw',
                                       allow_empty=True)

topic_processed = extractor_app.topic('feature_vectors', key_type=None,
                                      value_type=bytes)


@extractor_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    def parse_event(event: faust.events.EventT):
        key = event.key  # type: str
        value_bytes = event.value  # type: bytes

        try:
            value = loads(value_bytes)
            value["domain_name"] = key
            value["invalid_data"] = False
            return value
        except JSONDecodeError:
            return {"domain_name": key, "invalid_data": True}

    buffer = io.BytesIO()

    # Main message processing loop
    async for events_seq in stream.take_events(BATCH_SIZE, within=BATCH_TIMEOUT):  # type: Sequence[EventT]
        try:
            events_parsed = (parse_event(e) for e in events_seq)
            # We'll send the output into the partition of the first event in the batch
            partition = events_seq[0].message.partition
            # Reset the output buffer position
            buffer.seek(0)
            # Extract features and serialize the dataframe into a memory buffer
            extractor.extract_features(events_parsed, buffer)
            # Get the result bytes
            result_bytes = buffer.getbuffer()[0:buffer.tell()].tobytes()
            # Send the result
            await topic_processed.send(key=None, value=result_bytes, partition=partition)
        except Exception as e:
            keys = [e.key for e in events_seq] if events_seq is not None else None
            log_unhandled_error(e, EXTRACTOR, "-", all_keys=keys)
