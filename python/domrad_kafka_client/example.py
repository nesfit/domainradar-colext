import sys
from random import random

from domrad_kafka_client import ProcessorBase, run_client, AsyncProcessorBase
from domrad_kafka_client.types import SimpleMessage


class ExampleProcessor(ProcessorBase):
    def init(self) -> None:
        self._logger.info("Starting example processor")

    def process(self, key: bytes, value: bytes, partition: int, offset: int) -> list[SimpleMessage]:
        if not key:
            key = b''
        if not value:
            value = b''

        print(f"{offset}@{partition} | k: {key.decode()} / v: {value.decode()}", file=sys.stderr)
        return []


class ExampleAsyncProcessor(AsyncProcessorBase):

    def init(self) -> None:
        self._logger.info("Starting example async processor")

    async def process(self, key: bytes, value: bytes, partition: int, offset: int) -> list[SimpleMessage]:
        import asyncio

        await asyncio.sleep(random())
        if not key:
            key = b''
        if not value:
            value = b''

        print(f"{offset}@{partition} | k: {key.decode()} / v: {value.decode()}", file=sys.stderr)
        return []


if __name__ == '__main__':
    run_client("to_process_zone", ExampleAsyncProcessor)
