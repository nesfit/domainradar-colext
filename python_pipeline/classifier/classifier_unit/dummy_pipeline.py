from random import Random

import pandas as pd

from common.util import timestamp_now_millis


class Pipeline:
    def __init__(self):
        self._rnd = Random()

    def _rand_result(self, dn: str):
        result = {
            "domain_name": dn,
            "aggregate_probability": self._rnd.random(),
            "aggregate_description": "RANDOM VALUE AGGREGATE",
            "classification_results": [
                {
                    "classification_date": timestamp_now_millis(),
                    "classifier": "Phishing",
                    "probability": self._rnd.random(),
                    "description": "RANDOM VALUE PHISHING"
                },
                {
                    "classification_date": timestamp_now_millis(),
                    "classifier": "Malware",
                    "probability": self._rnd.random(),
                    "description": "RANDOM VALUE MALWARE"
                },
                {
                    "classification_date": timestamp_now_millis(),
                    "classifier": "DGA",
                    "probability": self._rnd.random(),
                    "description": "RANDOM VALUE DGA",
                    "details": {
                        "dga:fit": str(self._rnd.randint(0, 100)) + "%",
                        "dga:vut": str(self._rnd.randint(0, 100)) + "%",
                    }
                }
            ]
        }

        return result

    def classifyDomains(self, df: pd.DataFrame) -> list[dict]:
        ret = []
        for dn in df["domain_name"]:
            ret.append(self._rand_result(dn))

        return ret
