import abc
import os
from random import random
from time import sleep
import json
from datetime import datetime
import domrad_kafka_client.util
import pandas as pd
import pyarrow as pa
from domrad_kafka_client.types import SimpleMessage


class ClassifierBase(abc.ABC):
    @abc.abstractmethod
    def init(self) -> None:
        pass

    @abc.abstractmethod
    def classify(self, input_df: pd.DataFrame) -> list[dict]:
        pass


class ProductionClassifier(ClassifierBase):
    from classifiers.pipeline import Pipeline, PipelineOptions

    def __init__(self, **kwargs):
        self._options = ProductionClassifier.PipelineOptions(**kwargs)
        self._pipeline = None

        if not os.path.isdir(self._options.models_dir):
            raise ValueError(f"The models directory '{self._options.models_dir}' does not exist.")
        if not os.path.isdir(self._options.boundaries_dir):
            raise ValueError(f"The boundaries directory '{self._options.boundaries_dir}' does not exist.")

    def init(self):
        self._pipeline = ProductionClassifier.Pipeline(options=self._options)
        # Initialize the models by making a dummy classification request
        self.classify(pd.DataFrame({"domain_name": ["i.love.the.feta.cheese"]}))

    def classify(self, input_df: pd.DataFrame) -> list[dict]:
        return self._pipeline.classify_domains(input_df)


class DummyClassifier(ClassifierBase):
    def __init__(self, op_time_min: float = 1, op_time_max: float = 3):
        self._op_time_min = op_time_min
        self._op_time_delta = op_time_max - op_time_min

        if self._op_time_delta < 0 or self._op_time_delta < 0:
            raise ValueError(f"The operation time must be >= 0.")

    def init(self):
        sleep(self._random_time())

    def classify(self, input_df: pd.DataFrame) -> list[dict]:
        sleep(self._random_time())
        return [self._make_dummy_res(dn) for dn in input_df["domain_name"].tolist()]

    def _random_time(self):
        return self._op_time_min + random() * self._op_time_delta

    @staticmethod
    def _make_dummy_res(dn: str):
        return {
            "domain_name": dn,
            "aggregate_probability": random(),
            "aggregate_description": None,
            "timestamp": int(datetime.now().timestamp() * 1e3),
            "classification_results": [
                {
                    "category": 1,  # Phishing
                    "probability": random(),
                    "description": "TEST",
                    "details": {
                    }
                },
                {
                    "category": 2,  # Malware
                    "probability": random(),
                    "description": "TEST",
                    "details": {
                    }
                },
                {
                    "category": 3,  # DGA
                    "probability": random(),
                    "description": "TEST",
                    "details": {
                    }
                }
            ]
        }


def make_classifier_impl(config: dict) -> ClassifierBase:
    impl = config.get("client", {}).get("classifier_impl", "production")
    if impl == "dummy":
        return DummyClassifier(**config.get("dummmy-classifier", {}))
    elif impl == "production":
        return ProductionClassifier(**config.get("production-classifier", {}))
    else:
        raise ValueError(f"Invalid classifier implementation: {impl}. Valid values are 'dummy' and 'production'.")


class ClassifierProcessor(domrad_kafka_client.ProcessorBase):
    OUTPUT_TOPIC = "classification_results"

    def __init__(self, config: dict):
        super().__init__(config)
        self._pipeline = make_classifier_impl(self._config)

    def process(self, key: bytes, value: bytes, partition: int, offset: int) -> list[SimpleMessage]:
        ret = []

        try:
            df = pd.read_feather(pa.BufferReader(value))
        except Exception as e:
            self._logger.error("Cannot read value at p=%s, o=%s",
                               partition, offset, exc_info=e)
            return ret

        try:
            results = self._pipeline.classify(df)
        except Exception as e:
            return self._process_erroneous_df(partition, offset, df, e)

        result: dict
        for result in results:
            if "domain_name" not in result:
                self._logger.warning("Missing domain_name in a classification result at p=%s, o=%s",
                                     partition, offset)
                continue
            ret.append((self.OUTPUT_TOPIC, result["domain_name"].encode("utf-8"), self._serialize(result)))

        return ret

    def _process_erroneous_df(self, partition: int, offset: int, df: pd.DataFrame,
                              exc_info: Exception) -> list[SimpleMessage]:
        ret = []

        try:
            keys = df["domain_name"].tolist()
            self._logger.error("Unexpected classifiers exception at p=%s, o=%s. Keys: %s",
                               partition, offset, str(keys), exc_info=exc_info)

            for dn in keys:
                result = {
                    "domain_name": dn,
                    "aggregate_probability": -1,
                    "aggregate_description": "",
                    "timestamp": int(datetime.now().timestamp() * 1e3),
                    "error": str(exc_info)
                }

                ret.append((self.OUTPUT_TOPIC, dn.encode("utf-8"), self._serialize(result)))
        except Exception as internal_e:
            self._logger.error("Unexpected error when handling an exception at p=%s, o=%s",
                               partition, offset, exc_info=internal_e)

        return ret

    @staticmethod
    def _serialize(value: dict) -> bytes:
        return json.dumps(value, indent=None, separators=(',', ':')).encode("utf-8")
