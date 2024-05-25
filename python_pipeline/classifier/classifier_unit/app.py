"""app.py: The main module for the classifier unit component. Defines the Faust application."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import pandas as pd
import pyarrow as pa
from pandas import DataFrame

from common import read_config, make_app
from common.audit import log_unhandled_error, log_warning
from common.util import timestamp_now_millis
from .dummy_pipeline import Pipeline

CLASSIFIER = "classifier-unit"

# Read the config
config = read_config()
component_config = config.get(CLASSIFIER, {})

CONCURRENCY = component_config.get("concurrency", 4)
BATCH_SIZE = component_config.get("batch_size", 1)
BATCH_TIMEOUT = component_config.get("batch_timeout", 0)
USE_BATCHING = BATCH_SIZE > 1 and BATCH_TIMEOUT != 0

# The Faust application
classifier_app = make_app(CLASSIFIER, config)

# The input and output topics
topic_to_process = classifier_app.topic('feature_vectors', key_type=None,
                                        value_type=bytes, value_serializer='raw',
                                        allow_empty=False)

topic_processed = classifier_app.topic('classification_results', key_type=str, key_serializer='str',
                                       value_type=bytes, value_serializer='raw')

# The classification pipeline
pipeline = Pipeline()


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


async def process_dataframe(dataframe: DataFrame):
    try:
        results = pipeline.classifyDomains(dataframe)
        if not results:
            log_warning(CLASSIFIER, "No classification results were generated.", None)
            return

        for result in results:
            if "domain_name" not in result:
                log_warning(CLASSIFIER, "Missing domain_name in a classification result.", None)
                continue

            await topic_processed.send(key=result["domain_name"], value=result)
    except Exception as e:
        keys = dataframe["domain_name"].tolist()
        log_unhandled_error(e, CLASSIFIER, None, all_keys=keys)

        for dn in keys:
            result = make_error_result(dn, str(e))
            await topic_processed.send(key=dn, value=result)


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
