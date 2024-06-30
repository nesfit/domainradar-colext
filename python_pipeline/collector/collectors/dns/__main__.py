from common import main
from .dnscol import dns_app, COLLECTOR

if __name__ == '__main__':
    main(COLLECTOR, dns_app)
