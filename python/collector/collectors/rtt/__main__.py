"""__main__.py: The entry point file for running as a module using python -m."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from domrad_kafka_client import run_client
from .rtt import RTTProcessor

if __name__ == '__main__':
    run_client('to_process_IP', RTTProcessor)
