"""__main__.py: The main module for the feature extractor component. Provides a command-line interface for running
the Faust app. If a single command-line argument that contains a valid file path is passed, the script will load the
JSON file, extract features from it and print the result. Otherwise, the Faust app is started."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import importlib.util
import json
import os.path
import sys
from pprint import pprint

from pandas import DataFrame

from common import ensure_data_dir
from common.audit import log_unhandled_error
from . import extractor
from .app import extractor_app, EXTRACTOR


def extract_one(file):
    with open(file, 'r', encoding="utf8") as f:
        data = f.read()

    data = json.loads(data)
    extractor.init_transformations({})
    df: DataFrame
    if isinstance(data, list):
        df, errors = extractor.extract_features(data)
    else:
        df, errors = extractor.extract_features([data])

    if len(errors) > 0:
        print("Errors:")
        pprint(errors)
        print()

    if df is not None:
        print(f"Created {len(df)} feature vectors. Head:")
        pprint(df.head(n=2).to_dict(orient='records'))
        print()

        if importlib.util.find_spec("fastparquet") is None and importlib.util.find_spec("pyarrow") is None:
            print("The pyarrow library is not installed, the results won't be saved. Run `poetry install -E parquet`.")
            return

        output_file = os.path.splitext(file)[0] + ".parquet"
        df.to_parquet(output_file)
        print("Saved as: " + output_file)
    else:
        print("No feature vectors created.")


if __name__ == '__main__':
    if len(sys.argv) > 1 and os.path.isfile(sys.argv[1]):
        extract_one(sys.argv[1])
        exit()

    try:
        ensure_data_dir()
        extractor_app.main()
    except Exception as e:
        log_unhandled_error(e, EXTRACTOR)
        raise e
