from common import ensure_data_dir
from .rdap_ip import rdap_ip_app

if __name__ == '__main__':
    ensure_data_dir()
    rdap_ip_app.main()
