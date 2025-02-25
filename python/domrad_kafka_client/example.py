import sys
import time
from random import random

from domrad_kafka_client import ProcessorBase, run_client, AsyncProcessorBase, Message, TKey, TValue
from domrad_kafka_client.types import SimpleMessage


class ExampleProcessor(ProcessorBase[str, str]):

    def get_rl_bucket_key(self, message: Message[str, str]) -> str | None:
        if message.key:
            return message.key[0:1].lower()
        return None

    def deserialize(self, message: Message[TKey, TValue]):
        message.key = message.key_raw.decode() if message.key_raw is not None else None
        message.value = message.value_raw.decode() if message.value_raw is not None else None

    def process_error(self, message: Message[str, str], error: BaseException | int) -> list[SimpleMessage]:
        print(f">> {message.offset}@{message.partition} | k: {message.key} | Error: {error}")
        return []

    def process(self, message: Message[str, str]) -> list[SimpleMessage]:
        print(f">> {message.offset}@{message.partition} | k: {message.key} / v: {{message.value}} | {time.monotonic()}",
              file=sys.stderr)
        return []


class ExampleAsyncProcessor(ExampleProcessor):

    async def process(self, message: Message[str, str]) -> list[SimpleMessage]:
        import asyncio

        await asyncio.sleep(random())
        print(f"{message.offset}@{message.partition} | k: {message.key} / v: {{message.value}}", file=sys.stderr)
        return []


if __name__ == '__main__':
    run_client("to_process_zone", ExampleProcessor)
