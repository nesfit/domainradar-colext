"""app.py: The main module for the classifier unit component. Defines the Faust application."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import io

from faust.serializers import codecs
import pandas as pd
from pandas import DataFrame

from common import read_config, make_app, StringCodec

codecs.register("str", StringCodec())

CLASSIFIER = "classifier-unit"

# Read the config
config = read_config()
component_config = config.get(CLASSIFIER, {})

# The Faust application
classifier_app = make_app(CLASSIFIER, config)

CONCURRENCY = component_config.get("concurrency", 4)
BATCH_SIZE = component_config.get("batch_size", 1)
BATCH_TIMEOUT = component_config.get("batch_timeout", 0)

USE_BATCHING = BATCH_SIZE > 1 and BATCH_TIMEOUT != 0

# The input and output topics
topic_to_process = classifier_app.topic('feature_vectors', key_type=None,
                                        value_type=bytes, allow_empty=False)

topic_processed = classifier_app.topic('classification_results', key_type=str,
                                       value_type=bytes)


# The main loop
@classifier_app.agent(topic_to_process, concurrency=CONCURRENCY)
async def process_entries(stream):
    buf = io.BytesIO()
    current_df = None

    if USE_BATCHING:
        async for values_seq in stream.take(BATCH_SIZE, within=BATCH_TIMEOUT):
            value: bytes
            for value in values_seq:
                buf.seek(0)
                buf.write(value)
                df = pd.read_feather(buf)
                if current_df is None:
                    current_df = df
                else:
                    current_df = pd.concat([current_df, df])

            await process_dataframe(current_df)
    else:
        value: bytes
        async for value in stream:
            buf.seek(0)
            buf.write(value)
            df = pd.read_feather(buf)
            await process_dataframe(df)


async def process_dataframe(dataframe: DataFrame):
    # TODO
    print(dataframe)
