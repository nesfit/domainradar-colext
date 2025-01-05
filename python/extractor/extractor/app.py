"""app.py: The main module for the feature extractor component. Defines the Faust application."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import asyncio
import io
import json
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, Executor
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

BATCH_SIZE = component_config.get("batch_size", 50)
BATCH_TIMEOUT = component_config.get("batch_timeout", 5)
COMPUTATION_WORKERS = component_config.get("computation_workers", 1)
WORKER_SPAWN_METHOD = component_config.get("worker_spawn_method", "fork")
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

_pool_semaphore = asyncio.Semaphore()
_executor = None


def init_process_in_pool(the_config):
    """
    Initializes the worker processes in the multiprocessing pool.

    This function is called when a new worker process is created in the multiprocessing pool. It initializes the
    transformations in the worker process based on the configuration provided.

    Args:
        the_config (dict): The configuration dictionary for the extractor component.
    """
    import os
    import sys
    from extractor import extractor

    pid = os.getpid()
    print(f"Initializing worker ({pid})", file=sys.stderr)
    extractor.init_transformations(the_config)


async def ensure_pool() -> Executor:
    """
    Ensures that a pool of workers is available for parallel computation.

    This asynchronous method checks if a pool of workers has already been created. If not, it creates a new pool.
    The number of workers in the pool is determined by the `COMPUTATION_WORKERS` configuration option. If this option
    is set to 1, no pool is created and the method returns None.

    The method uses a semaphore to ensure that the pool is not initialized multiple times concurrently. The type of
    the pool (process pool or thread pool) is determined by the `WORKER_SPAWN_METHOD` configuration option.

    Returns:
        Executor: The created pool of workers, or None if `COMPUTATION_WORKERS` is set to 1.

    Raises:
        ValueError: If `WORKER_SPAWN_METHOD` is not 'thread' or a valid multiprocessing context.
    """
    global _executor
    await _pool_semaphore.acquire()
    if COMPUTATION_WORKERS > 1:
        if _executor is not None:
            _pool_semaphore.release()
            return _executor
        logger.info("Initializing the extractor workers")
        if WORKER_SPAWN_METHOD == "thread":
            _executor = ThreadPoolExecutor(COMPUTATION_WORKERS)
        else:
            context = multiprocessing.get_context(WORKER_SPAWN_METHOD)
            _executor = ProcessPoolExecutor(COMPUTATION_WORKERS, mp_context=context,
                                            initializer=init_process_in_pool, initargs=(component_config,))
    else:
        _executor = None
    _pool_semaphore.release()
    return _executor


@extractor_app.agent(topic_to_process, concurrency=COMPUTATION_WORKERS)
async def process_entries(stream):
    # Create the executor for parallel computation
    # This is called from each "concurrency" shard but only the first will actually create the pool
    executor = await ensure_pool()

    def parse_event(event: faust.events.EventT):
        """
        Parses a single event from the input stream.

        This function takes a single event from the input stream and extracts the key and value from it. The value is
        then deserialized from bytes to a dictionary. If the deserialization fails, the dictionary contains the key
        and a flag under the 'invalid_data' key indicating that the data was invalid.

        Args:
            event (faust.events.EventT): The event to parse.

        Returns:
            dict[str, Any]: The parsed event as a dictionary.
        """
        msg_key = event.key  # type: str
        value_bytes = event.value  # type: bytes

        if value_bytes is None:
            # Empty entries will be ignored
            return None
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
            # Extract features
            if COMPUTATION_WORKERS > 1:
                # Run the extraction in a separate thread or process
                df, errors = await extractor_app.loop.run_in_executor(
                    executor, extractor.extract_features, list(events_parsed))
            else:
                df, errors = extractor.extract_features(events_parsed)

            if df is not None:
                if PRODUCE_DFS:
                    # Reset the output buffer position
                    buffer.seek(0)
                    # Serialize the dataframe into a memory buffer
                    # noinspection PyTypeChecker
                    df.to_feather(buffer)
                    # Get the result bytes
                    result_bytes = buffer.getbuffer()[0:buffer.tell()].tobytes()
                    # Send the result
                    await topic_processed.send(key=None, value=result_bytes)

                if PRODUCE_JSONS:
                    # Get rid of the only Timedelta object in the vector (temporary, will be addressed globally)
                    df["rdap_registration_period"] = df["rdap_registration_period"].dt.total_seconds() / 60

                    # Serialize the dataframe into a JSON array and produce the vectors as individual messages
                    for _, row in df.iterrows():
                        row_json = row.to_json(orient='index')
                        await topic_processed_jsons.send(key=row["domain_name"].encode("utf-8"),
                                                         value=row_json.encode("utf-8"))

            if len(errors) > 0:
                for key, error in errors.items():
                    logger.k_unhandled_error(error, key)
        except Exception as e:
            keys = [e.key for e in events_seq] if events_seq is not None else None
            logger.k_unhandled_error(e, None, all_keys=keys)

    if executor:
        executor.shutdown(True, cancel_futures=True)
