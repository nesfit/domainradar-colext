Kafka Multiprocessor
====================

Purpose
-------
This library runs a Kafka consumer that hands messages off to multiple worker
processes, then produces zero or more output messages per input message with
transactional, exactly-once style commits. It is meant for "load -> transform ->
publish" pipelines where processing is CPU- or IO-heavy and parallelism is needed.

Features
-------------
- Multiprocess message processing with a configurable worker pool.
- One input topic, many output messages per input message.
- Transactional producer + offset commit per input message.
- Optional rate limiting per message (Redis-backed) for external API calls.
- Synchronous or async processor implementations.
- Standalone CLI mode to test processors without Kafka.

How it works
------------
1) The client consumes from the input topic.
2) Each message is queued into a worker process.
3) Workers deserialize and process the message via your processor class.
4) Results are returned to the manager in a "reorder buffer" (ROB) per partition.
5) The manager publishes results and commits the input offset in a transaction.
   This keeps output publishing and offset commits in lockstep.

Configuration
-------------
The library reads a TOML config from `APP_CONFIG_FILE` (default: `./config.toml`).
Below is a minimal example. Add more `consumer` and `producer` options as needed.

```toml
[connection]
brokers = ["kafka-1:9092", "kafka-2:9092"]
use_ssl = false

[client]
app_id = "my-app"
workers = 4
poll_timeout = 1.0
init_wait = 0.0
max_queued_items = 100
resume_after_freed_items = 10
enable_rate_limiter = false
mp_start_method = "forkserver"

[consumer]
auto.offset.reset = "earliest"

[producer]
acks = "all"
```

If `use_ssl = true`, also configure:

```toml
[connection.ssl]
ca_file = "/path/ca.crt"
client_cert_file = "/path/client.crt"
client_key_file = "/path/client.key"
client_key_password = "secret"
check_hostname = true
```

Usage (sync processor)
----------------------
Create a processor that implements `deserialize`, `process`, and `process_error`,
then call `run_client`.

```python
from kafka_multiprocessor import run_client, SyncKafkaMessageProcessor, Message

class ExampleProcessor(SyncKafkaMessageProcessor[dict, dict]):
    def deserialize(self, message: Message[dict, dict]) -> None:
        import json
        message.key = json.loads(message.key_raw or b"null")
        message.value = json.loads(message.value_raw or b"null")

    def process(self, message: Message[dict, dict]):
        out_key = str(message.key).encode("utf-8")
        out_val = str(message.value).encode("utf-8")
        return [("output-topic", out_key, out_val)]

    def process_error(self, message: Message[dict, dict], error):
        # Return error messages or [] to drop
        return []

if __name__ == "__main__":
    run_client("input-topic", ExampleProcessor)
```

Usage (async processor)
-----------------------
Async processors use the same interface but `process` is `async`.

```python
from kafka_multiprocessor import run_client, AsyncKafkaMessageProcessor, Message

class ExampleAsyncProcessor(AsyncKafkaMessageProcessor[str, str]):
    def deserialize(self, message: Message[str, str]) -> None:
        message.key = (message.key_raw or b"").decode("utf-8")
        message.value = (message.value_raw or b"").decode("utf-8")

    async def process(self, message: Message[str, str]):
        return [("output-topic", message.key.encode("utf-8"), message.value.encode("utf-8"))]

    def process_error(self, message: Message[str, str], error):
        return []

if __name__ == "__main__":
    run_client("input-topic", ExampleAsyncProcessor)
```

Standalone CLI mode
-------------------
Set `DOMRAD_PROCESS_STANDALONE=1` to run your processor on a single input
message without Kafka. The CLI lets you pass key/value as literals, files, or
stdin.

```bash
export APP_CONFIG_FILE=./config.toml
export DOMRAD_PROCESS_STANDALONE=1
python path/to/your_app.py --key '{"id":1}' --value '{"payload":"hi"}'
```

Options:
- `--key` / `--key-file` / `--no-key`
- `--value` / `--value-file` / `--no-value`

Rate limiting
-------------
If `client.enable_rate_limiter = true`, workers will apply rate limiting using
Redis and `pyrate-limiter`. Configure per-bucket limits under `rate_limiter`.
Your processor decides the bucket by overriding `get_rl_bucket_key`.

```toml
[client]
enable_rate_limiter = true
redis_uri = "redis://localhost:6379/0"

[rate_limiter.default]
immediate = false
max_wait = 10
rates = [
  { requests = 10, interval_ms = 1000 },
]
```

Implementation notes
--------------------
- Messages are processed per partition and re-ordered to preserve commit order.
- If a message sits too long in the ROB, the process will log and eventually
  terminate (see `client.entry_late_warning_threshold` and
  `client.max_entry_time_in_queue`).
