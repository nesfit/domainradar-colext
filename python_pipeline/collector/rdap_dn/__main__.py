from common.util import ensure_data_dir
from .rdap_dn import rdap_dn_app

if __name__ == '__main__':
    ensure_data_dir()
    rdap_dn_app.main()
