from common import ensure_data_dir
from common.audit import log_unhandled_error
from .rdap_dn import rdap_dn_app, COLLECTOR

if __name__ == '__main__':
    try:
        ensure_data_dir()
        rdap_dn_app.main()
    except Exception as e:
        log_unhandled_error(e, COLLECTOR)
        raise e
