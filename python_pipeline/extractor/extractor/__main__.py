import json
import os.path
import sys
from pprint import pprint

from common import ensure_data_dir
from common.audit import log_unhandled_error
from .app import extractor_app, EXTRACTOR
from . import extractor


def extract_one(file):
    with open(file, 'r', encoding="utf8") as f:
        data = f.read()

    data = json.loads(data)
    extractor.init_transformations({})
    if isinstance(data, list):
        df, _ = extractor.extract_features(data)
    else:
        df, _ = extractor.extract_features([data])

    pprint(df.to_dict(orient='records'))


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
