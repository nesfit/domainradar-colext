from common import ensure_data_dir
from common.audit import log_unhandled_error
from .rdap_ip import rdap_ip_app, COLLECTOR

if __name__ == '__main__':
    try:
        ensure_data_dir()
        rdap_ip_app.main()
    except Exception as e:
        log_unhandled_error(e, COLLECTOR)
        raise e
