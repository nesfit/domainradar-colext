"""app.py: The main module for the feature extractor component. Defines the Faust application."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import io
import json
from concurrent.futures import ThreadPoolExecutor
from json import loads, JSONDecodeError
from typing import Sequence

import faust.events
from faust import EventT

from common import read_config, make_app, log
from . import extractor

EXTRACTOR = "extractor"
COMPONENT_NAME = EXTRACTOR

# Read the config
config = read_config()
component_config = config.get(EXTRACTOR, {})
logger = log.init(COMPONENT_NAME, config)

logger.k_warning("Yolo", "key", my="arg")

CONCURRENCY = component_config.get("concurrency", 4)
BATCH_SIZE = component_config.get("batch_size", 50)
BATCH_TIMEOUT = component_config.get("batch_timeout", 5)
COMPUTATION_THREADS = component_config.get("computation_threads", 1)
PRODUCE_JSONS = component_config.get("produce_jsons", False)
PRODUCE_DFS = not component_config.get("only_produce_jsons", False)

if not PRODUCE_JSONS and not PRODUCE_DFS:
    logger.error("The 'only_produce_jsons' option is set, but 'produce_jsons' is not. Refusing to operate.")
    exit(1)

# Init the list of transformations
extractor.init_transformations(component_config)

# The Faust application
extractor_app = make_app(EXTRACTOR, config, COMPONENT_NAME)

# The input and output topics
# Let's deserialize the result into a dict manually
topic_to_process = extractor_app.topic('all_collected_data', key_type=str, key_serializer='str',
                                       value_type=bytes, value_serializer='raw', allow_empty=True)

if PRODUCE_DFS:
    topic_processed = extractor_app.topic('feature_vectors', key_type=None,
                                          value_type=bytes, value_serializer='raw')
if PRODUCE_JSONS:
    topic_processed_jsons = extractor_app.topic('feature_vectors_json', key_type=str, key_serializer='str',
                                                value_type=bytes, value_serializer='raw')


@extractor_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    executor = ThreadPoolExecutor(COMPUTATION_THREADS) if COMPUTATION_THREADS > 1 else None

    def parse_event(event: faust.events.EventT):
        msg_key = event.key  # type: str
        value_bytes = event.value  # type: bytes

        try:
            value = loads(value_bytes)
            value["domain_name"] = msg_key
            value["invalid_data"] = False
            return value
        except JSONDecodeError:
            return {"domain_name": msg_key, "invalid_data": True}

    buffer = io.BytesIO()

    # Main message processing loop
    async for events_seq in stream.take_events(BATCH_SIZE, within=BATCH_TIMEOUT):  # type: Sequence[EventT]
        try:
            events_parsed = (parse_event(e) for e in events_seq)
            # Reset the output buffer position
            buffer.seek(0)
            # Extract features
            if COMPUTATION_THREADS > 1:
                # We can run the pandas code in a separate thread, although currently, the code does not really release
                # the GIL, so it's likely not going to make any difference
                df, errors = await extractor_app.loop.run_in_executor(
                    executor, extractor.extract_features, events_parsed)
            else:
                df, errors = extractor.extract_features(events_parsed)

            if df is not None:
                if PRODUCE_DFS:
                    # Serialize the dataframe into a memory buffer
                    # noinspection PyTypeChecker
                    df.to_feather(buffer)
                    # Get the result bytes
                    result_bytes = buffer.getbuffer()[0:buffer.tell()].tobytes()
                    # Send the result
                    await topic_processed.send(key=None, value=result_bytes)

                if PRODUCE_JSONS:
                    # Serialize the dataframe into a JSON array and produce the vectors as individual messages
                    df_dict = df.to_dict(orient='records')
                    for row in df_dict:
                        # Get rid of the only Timedelta object in the vector (temporary, will be addressed globally)
                        row["rdap_registration_period"] = row["rdap_registration_period"].total_seconds() / 60
                        row_json = json.dumps(row, indent=None, separators=(',', ':'))
                        await topic_processed_jsons.send(key=row["domain_name"].encode("utf-8"),
                                                         value=row_json.encode("utf-8"))

            if len(errors) > 0:
                for key, error in errors.items():
                    logger.k_unhandled_error(error, key)
        except Exception as e:
            keys = [e.key for e in events_seq] if events_seq is not None else None
            logger.k_unhandled_error(e, None, all_keys=keys)
