"""app.py: The main module for the classifier unit component. Defines the Faust application."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import os.path
from concurrent.futures import ThreadPoolExecutor
from json import dumps
from queue import SimpleQueue

import pandas as pd
import pyarrow as pa
from classifiers.options import PipelineOptions
from classifiers.pipeline import Pipeline
from pandas import DataFrame

from common import read_config, make_app
from common.audit import log_unhandled_error, log_warning
from common.util import timestamp_now_millis

CLASSIFIER = "classifier-unit"

# Read the config
config = read_config()
component_config = config.get(CLASSIFIER, {})

MODEL_PATH = component_config.get("model_path")
CONCURRENCY = component_config.get("concurrency", 4)
CLASSIFIER_WORKERS = component_config.get("classifier_workers", 64)
BATCH_SIZE = component_config.get("batch_size", 1)
BATCH_TIMEOUT = component_config.get("batch_timeout", 0)
USE_BATCHING = BATCH_SIZE > 1 and BATCH_TIMEOUT != 0

pipeline_options = PipelineOptions(MODEL_PATH)

if not os.path.isdir(pipeline_options.models_dir):
    raise ValueError(f"The models directory '{pipeline_options.models_dir}' does not exist.")
if not os.path.isdir(pipeline_options.boundaries_dir):
    raise ValueError(f"The boundaries directory '{pipeline_options.boundaries_dir}' does not exist.")

# The Faust application
classifier_app = make_app(CLASSIFIER, config)

# The input and output topics
topic_to_process = classifier_app.topic('feature_vectors', key_type=None,
                                        value_type=bytes, value_serializer='raw',
                                        allow_empty=False)

topic_processed = classifier_app.topic('classification_results', key_type=str, key_serializer='str',
                                       value_type=bytes, value_serializer='raw')

# The classification pipelines
pipelines_queue = SimpleQueue()
for i in range(CLASSIFIER_WORKERS):
    classifier_app.log.info(f"Initializing pipeline worker #{i}")
    pipelines_queue.put(Pipeline(pipeline_options))

executor = ThreadPoolExecutor(max_workers=CLASSIFIER_WORKERS)


# The main loop
@classifier_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    if USE_BATCHING:
        current_df = None
        async for values_seq in stream.take(BATCH_SIZE, within=BATCH_TIMEOUT):
            value: bytes
            for value in values_seq:
                df = pd.read_feather(pa.BufferReader(value))
                if current_df is None:
                    current_df = df
                else:
                    current_df = pd.concat([current_df, df])

            await process_dataframe(current_df)
    else:
        value: bytes
        async for value in stream:
            df = pd.read_feather(pa.BufferReader(value))
            await process_dataframe(df)

    executor.shutdown(True, cancel_futures=True)


def serialize(value: dict) -> bytes:
    return dumps(value, indent=None, separators=(',', ':')).encode("utf-8")


def pick_classifier_and_classify(queue: SimpleQueue, dataframe: DataFrame):
    pipeline = queue.get(block=True)

    try:
        results = pipeline.classify_domains(dataframe)
    except Exception as e:
        keys = dataframe["domain_name"].tolist()
        log_unhandled_error(e, CLASSIFIER, None, all_keys=keys)
        results = []
    finally:
        queue.put(pipeline)

    return results


async def process_dataframe(dataframe: DataFrame):
    try:
        results = await classifier_app.loop.run_in_executor(executor, pick_classifier_and_classify,
                                                            pipelines_queue, dataframe)
        if not results:
            log_warning(CLASSIFIER, "No classification results were generated.", None)
            return

        for result in results:
            if "domain_name" not in result:
                log_warning(CLASSIFIER, "Missing domain_name in a classification result.", None)
                continue

            await topic_processed.send(key=result["domain_name"], value=serialize(result))
    except Exception as e:
        keys = dataframe["domain_name"].tolist()
        log_unhandled_error(e, CLASSIFIER, None, all_keys=keys)

        for dn in keys:
            result = make_error_result(dn, str(e))
            await topic_processed.send(key=dn, value=serialize(result))


def make_error_result(dn: str, error_message: str):
    return {
        "domain_name": dn,
        "aggregate_probability": -1,
        "aggregate_description": "",
        "classification_results": [
            {
                "classification_date": timestamp_now_millis(),
                "classifier": "Error",
                "probability": -1,
                "description": "An error occurred while performing the classification.",
                "details": {
                    "error": error_message
                }
            }
        ]
    }
