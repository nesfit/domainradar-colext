from common import ensure_data_dir
from common.audit import log_unhandled_error
from .app import extractor_app, EXTRACTOR

if __name__ == '__main__':
    try:
        ensure_data_dir()
        extractor_app.main()
    except Exception as e:
        log_unhandled_error(e, EXTRACTOR)
        raise e
