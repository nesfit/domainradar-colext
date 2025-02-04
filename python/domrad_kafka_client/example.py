import os
import sys

from domrad_kafka_client import ProcessorBase, run_client
from domrad_kafka_client.types import SimpleMessage


class ExampleProcessor(ProcessorBase):
    def init(self) -> None:
        pass

    def process(self, key: bytes, value: bytes, partition: int, offset: int) -> list[SimpleMessage]:
        print(f"{offset}@{partition} | k: {key.decode()} / v: {value.decode()}", file=sys.stderr)
        return []

if __name__ == '__main__':
    run_client("to_process_zone", ExampleProcessor)