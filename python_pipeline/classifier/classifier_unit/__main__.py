from common import ensure_data_dir
from common.audit import log_unhandled_error
from .app import classifier_app, CLASSIFIER

if __name__ == '__main__':
    try:
        ensure_data_dir()
        classifier_app.main()
    except Exception as e:
        log_unhandled_error(e, CLASSIFIER)
        raise e
