"""__main__.py: The entry point file for running as a module using python -m."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from kafka_multiprocessor import run_client
from .dnscol import DNSProcessor, COLLECTOR

if __name__ == '__main__':
    run_client(
        'to_process_DNS', 
        DNSProcessor, 
        lambda cfg: cfg.get(COLLECTOR, {}).get("app_id")
    )
