import abc
import os
from datetime import datetime
from random import random
from time import sleep

import pandas as pd


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
