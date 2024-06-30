from common import main
from .rdap_ip import COLLECTOR, rdap_ip_app

if __name__ == '__main__':
    main(COLLECTOR, rdap_ip_app)
