import argparse
import logging
import sys
from dataclasses import dataclass
from typing import Type

from .message_processor import KafkaMessageProcessor, AsyncKafkaMessageProcessor, Message
from .util import add_logging_trace_level


@dataclass
class InputSpec:
    kind: str  # "literal" | "file" | "stdin" | "null"
    value: str | None  # literal value, file path, or "-"


def read_input(spec: InputSpec) -> bytes | None:
    if spec.kind == "literal":
        return spec.value.encode("utf-8")
    if spec.kind == "file":
        with open(spec.value, "rb") as f:
            return f.read()
    if spec.kind == "null":
        return None
    # spec.kind == "stdin"
    return sys.stdin.read().encode("utf-8")


def run_cli(config: dict, processor_type: Type[KafkaMessageProcessor]):
    p = argparse.ArgumentParser(
        description="Accept key/value from literal, file, or stdin via file options."
    )

    # key: exactly one of --key or --key-file is required
    g_key = p.add_mutually_exclusive_group(required=True)
    g_key.add_argument("--key", help="Key as a literal string")
    g_key.add_argument("--key-file", help="File containing key, or '-' for stdin")
    g_key.add_argument("--no-key", action="store_true")

    # value: exactly one of --value or --value-file is required
    g_val = p.add_mutually_exclusive_group(required=True)
    g_val.add_argument("--value", help="Value as a literal string")
    g_val.add_argument("--value-file", help="File containing value, or '-' for stdin")
    g_val.add_argument("--no-value", action="store_true")

    p.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    args = p.parse_args()

    logging.basicConfig(level=args.log_level)
    add_logging_trace_level()

    # Build specs
    key_spec = (
        InputSpec("null", None)
        if args.no_key
        else (
            InputSpec("stdin", "-")
            if args.key_file == "-"
            else (
                InputSpec("file", args.key_file)
                if args.key_file is not None
                else InputSpec("literal", args.key)
            )
        )
    )
    value_spec = (
        InputSpec("null", None)
        if args.no_value
        else (
            InputSpec("stdin", "-")
            if args.value_file == "-"
            else (
                InputSpec("file", args.value_file)
                if args.value_file is not None
                else InputSpec("literal", args.value)
            )
        )
    )

    # Disallow consuming stdin twice
    if key_spec.kind == "stdin" and value_spec.kind == "stdin":
        p.error("key and value cannot both come from stdin ('-').")

    key_data = read_input(key_spec)
    value_data = read_input(value_spec)
    message = Message(key_data, value_data, None, None, 0, 0)

    processor = processor_type(config)
    if isinstance(processor, AsyncKafkaMessageProcessor):
        import asyncio

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(processor.init_async())
        processor.deserialize(message)
        ret = loop.run_until_complete(processor.process(message))
        loop.run_until_complete(processor.close_async())
        loop.close()
    else:
        processor.deserialize(message)
        ret = processor.process(message)

    for out_topic, out_key, out_value in ret:
        print(f"--- Output to topic '{out_topic}' ---")
        if out_key is not None:
            print(f"Key ({len(out_key)} bytes):")
            print(out_key.decode("utf-8", "replace"))
        else:
            print("Key: None")
        if out_value is not None:
            print(f"Value ({len(out_value)} bytes):")
            print(out_value.decode("utf-8", "replace"))
        else:
            print("Value: None")
