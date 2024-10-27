"""app.py: The main module for the classifier unit component. Defines the Faust application."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import asyncio
import multiprocessing
import os.path
from concurrent.futures import ProcessPoolExecutor, Executor
from json import dumps

import pandas as pd
import pyarrow as pa
from classifiers.options import PipelineOptions
from scipy.constants import milli

from common import read_config, make_app, log
from common.util import timestamp_now_millis

CLASSIFIER = "classifier-unit"
COMPONENT_NAME = CLASSIFIER

# Read the config
config = read_config()
component_config = config.get(CLASSIFIER, {})
logger = log.init(COMPONENT_NAME, config)

MODEL_PATH = component_config.get("model_path")
CLASSIFIER_WORKERS = component_config.get("classifier_workers", 8)
WORKER_SPAWN_METHOD = component_config.get("worker_spawn_method", "fork")

pipeline_options = PipelineOptions(MODEL_PATH)

if not os.path.isdir(pipeline_options.models_dir):
    raise ValueError(f"The models directory '{pipeline_options.models_dir}' does not exist.")
if not os.path.isdir(pipeline_options.boundaries_dir):
    raise ValueError(f"The boundaries directory '{pipeline_options.boundaries_dir}' does not exist.")


def init_classifier(the_options):
    from . import classifier
    import sys
    import os
    print(f"Initializing classifier worker process (PID {os.getpid()})", file=sys.stderr)
    classifier.init_pipeline(the_options)
    print(f"Init done (PID {os.getpid()})", file=sys.stderr)


def process_input_value(value: bytes | None) -> list[dict] | str:
    from . import classifier
    import os
    import pandas
    import pyarrow
    if value is None:
        return f"{os.getpid()}: No data provided"

    try:
        df = pandas.read_feather(pyarrow.BufferReader(value))
        return classifier.pipeline.classify_domains(df)
    except Exception as e:
        return str(e)


# The Faust application
classifier_unit_app = make_app(CLASSIFIER, config, COMPONENT_NAME)

# The input and output topics
topic_to_process = classifier_unit_app.topic('feature_vectors', key_type=None,
                                             value_type=bytes, value_serializer='raw',
                                             allow_empty=False)

topic_processed = classifier_unit_app.topic('classification_results', key_type=str, key_serializer='str',
                                            value_type=bytes, value_serializer='raw')

_pool_semaphore = asyncio.Semaphore()
_executor: Executor | None = None


async def ensure_pool() -> Executor:
    global _executor
    await _pool_semaphore.acquire()
    if CLASSIFIER_WORKERS > 0:
        if _executor is not None:
            _pool_semaphore.release()
            return _executor
        logger.info("Initializing the classifier workers")
        context = multiprocessing.get_context(WORKER_SPAWN_METHOD)
        _executor = ProcessPoolExecutor(max_workers=CLASSIFIER_WORKERS, mp_context=context,
                                        initializer=init_classifier, initargs=(pipeline_options,))

        wait_futures = [classifier_unit_app.loop.run_in_executor(_executor, process_input_value, None) for _ in
                        range(CLASSIFIER_WORKERS)]
        await asyncio.wait(wait_futures)
        logger.info("Classifiers initialized")
    else:
        init_classifier(pipeline_options)
        _executor = None
    _pool_semaphore.release()
    return _executor


# The main loop
@classifier_unit_app.agent(topic_to_process, concurrency=CLASSIFIER_WORKERS)
async def process_entries(stream):
    async def do_error(value: bytes, exc_info):
        try:
            df = pd.read_feather(pa.BufferReader(value))
            keys = df["domain_name"].tolist()
            if exc_info is None:
                logger.k_warning("Input processing error", None, all_keys=keys)
            else:
                logger.k_unhandled_error(exc_info, None, all_keys=keys)

            for dn in keys:
                result = make_error_result(dn, str(e))
                await topic_processed.send(key=dn, value=serialize(result))
        except Exception as internal_e:
            logger.error("An error occurred while handling an error...", exc_info=internal_e)

    executor = await ensure_pool()
    value: bytes
    async for value in stream:
        try:
            if executor:
                results = await classifier_unit_app.loop.run_in_executor(executor, process_input_value, value)
            else:
                results = process_input_value(value)

            if isinstance(results, str):
                logger.error("An error occurred while processing the input: %s", results)
                await do_error(value, None)
                continue

            for result in results:
                if "domain_name" not in result:
                    logger.warning("Missing domain_name in a classification result")
                    continue

                await topic_processed.send(key=result["domain_name"], value=serialize(result))
        except Exception as e:
            await do_error(value, e)

    if executor:
        executor.shutdown(True, cancel_futures=True)


def serialize(value: dict) -> bytes:
    return dumps(value, indent=None, separators=(',', ':')).encode("utf-8")


def make_error_result(dn: str, error_message: str):
    return {
        "domain_name": dn,
        "aggregate_probability": -1,
        "aggregate_description": "",
        "timestamp": timestamp_now_millis(),
        "error": error_message
    }
