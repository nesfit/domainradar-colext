import asyncio
import json
import logging
import logging.config
import multiprocessing as mp
import os
import queue
import signal
from inspect import isawaitable
from time import sleep

import redis
from typing import Type, Union, Awaitable

from pyrate_limiter import BucketFactory, RateItem, AbstractBucket, TimeClock, RedisBucket, Rate, Limiter, \
    LimiterDelayException, BucketFullException, InMemoryBucket, MonotonicClock

from .message_processor import KafkaMessageProcessor, AsyncKafkaMessageProcessor, Message, SyncKafkaMessageProcessor

_process: mp.Process | None = None


def _get_rate_limiter_section(config: dict, bucket_key: str) -> dict | None:
    """
    Get the rate limiter configuration section for the given bucket key.
    :param config: The application configuration.
    :param bucket_key: The bucket key to get the configuration for.
    :return: The rate limiter configuration section for the given bucket key or None if not found (or not a dictionary).
    """
    assert bucket_key is not None
    assert config is not None

    rate_limiter_config = config.get("rate_limiter", {})
    rl_config_for_key = rate_limiter_config.get(bucket_key)
    if (rl_config_for_key is None or not isinstance(rl_config_for_key, dict)) and bucket_key is not "default":
        rl_config_for_key = rate_limiter_config.get("default", None)
    if not isinstance(rl_config_for_key, dict):
        return None
    return rl_config_for_key


class _LoopError(Exception):
    def __init__(self, error_messages_to_send: list | None = None):
        self._error_messages_to_send = error_messages_to_send

    @property
    def error_messages_to_send(self) -> list | None:
        return self._error_messages_to_send


class _CustomBucketFactory(BucketFactory):

    def __init__(self, config: dict, redis_pool: redis.ConnectionPool | redis.asyncio.ConnectionPool) -> None:
        super().__init__()
        self._config = config
        self._clock = MonotonicClock()
        self._buckets: dict[str, AbstractBucket | Awaitable[AbstractBucket]] = {}
        self._pool = redis_pool

    def get_rates(self, bucket_key: str):
        rl_config_for_key = _get_rate_limiter_section(self._config, bucket_key)
        if rl_config_for_key is None:
            return []
        config = rl_config_for_key.get("rates", [])
        if config is None or not isinstance(config, list):
            return []

        return [Rate(x["requests"], x["interval_ms"]) for x in config]

    def wrap_item(self, name: str, weight: int = 1) -> Union[RateItem, Awaitable[RateItem]]:
        return RateItem(name=name, timestamp=self._clock.now(), weight=1)

    def get(self, item: RateItem) -> Union[AbstractBucket, Awaitable[AbstractBucket]]:
        bucket = self._buckets.get(item.name, None)
        if bucket is not None: return bucket

        # b = InMemoryBucket(self.get_rates(item.name))
        # self.schedule_leak(b, self._clock)
        # self._buckets[item.name] = b
        # return b

        if isinstance(self._pool, redis.asyncio.ConnectionPool):
            async def make_bucket():
                async_redis_db = redis.asyncio.Redis(connection_pool=self._pool, db=0)
                async_bucket = await RedisBucket.init(self.get_rates(item.name), async_redis_db, item.name)
                self.schedule_leak(async_bucket, self._clock)
                self._buckets[item.name] = async_bucket
                return async_bucket

            return make_bucket()
        else:
            redis_db = redis.Redis(connection_pool=self._pool, db=0)
            bucket = RedisBucket.init(self.get_rates(item.name), redis_db, item.name)
            self.schedule_leak(bucket, self._clock)
            self._buckets[item.name] = bucket
            return bucket


class WorkerProcess:
    def __init__(self, worker_id: int, config: dict, processor_type: Type[KafkaMessageProcessor],
                 to_process: mp.Queue, processed: mp.Queue):
        self._config = config
        self._logger = logging.getLogger(f"worker")
        self._to_process = to_process
        self._processed = processed

        self._limiters: dict[str, Limiter] = {}
        self._bucket_factory = None
        if config.get("client", {}).get("enable_rate_limiter"):
            self._init_rate_limiting()

        self._running = True
        self._logger.info("Initializing worker")
        self._processor = processor_type(config)

    def run(self):
        while self._running:
            partition, offset, key_bytes, value_bytes = None, None, None, None

            # Try consuming a message
            # Watch for interrupts and errors
            try:
                partition, offset, key_bytes, value_bytes = self._to_process.get(True, 5.0)
                self._logger.debug("Processing at p=%s, o=%s", partition, offset)
            except queue.Empty:
                continue
            except KeyboardInterrupt:
                self._logger.info("Interrupted. Shutting down")
                self._running = False
            except Exception as e:
                self._logger.error("Unexpected error. Shutting down", exc_info=e)
                self._running = False

            # A message has been consumed from the input queue
            # We must ensure it gets back to the processed queue
            if partition is not None:
                ret = None

                try:
                    # Though if cancellation was requested, don't actually classify the input
                    if self._running:
                        ret = self._process(key_bytes, value_bytes, partition, offset)
                        assert not isawaitable(ret)
                except KeyboardInterrupt:
                    self._logger.info("Interrupted. Shutting down")
                    self._running = False
                except _LoopError as e:
                    self._logger.debug("Processed with error at p=%s, o=%s", partition, offset,
                                       exc_info=e.__cause__)
                    ret = e.error_messages_to_send
                except Exception as e:
                    self._logger.error("Unexpected error. Shutting down", exc_info=e)
                    self._running = False
                finally:
                    self._processed.put((partition, offset, ret or []), True, None)
                    self._logger.debug("Placed to processed q at p=%s, o=%s", partition, offset)

        self._logger.info("Finished (PID %s)", os.getpid())

    def close(self):
        self._running = False

    def _process(self, key_bytes, value_bytes, partition, offset) -> list | Awaitable[list]:
        message = Message(key_bytes, value_bytes, None, None, partition, offset)

        # Deserialize the message using user code
        # The user code should populate the 'key' and 'value' fields of the Message object
        try:
            self._processor.deserialize(message)
        except Exception as e:
            ret = self._processor.process_error(message, e)
            raise _LoopError(ret) from e

        # Rate limiting
        if self.rate_limiter_enabled:
            rl_result = self._do_rate_limit(message)
            if rl_result != 0:
                ret = self._processor.process_error(message, rl_result)
                raise _LoopError(ret)

        # User processing function
        try:
            ret = self._processor.process(message)
            self._logger.debug("Processed at p=%s, o=%s", partition, offset)
            return ret
        except Exception as e:
            ret = self._processor.process_error(message, e)
            raise _LoopError(ret) from e

    def _init_rate_limiting(self):
        redis_pool = redis.ConnectionPool.from_url(self._config.get("client", {}).get("redis_uri"))
        self._bucket_factory = _CustomBucketFactory(config=self._config, redis_pool=redis_pool)

    def _get_limiter(self, bucket_key: str) -> Limiter:
        limiter = self._limiters.get(bucket_key, None)
        if limiter is not None: return limiter

        rl_config_for_key = _get_rate_limiter_section(self._config, bucket_key)
        imm = rl_config_for_key.get("immediate", False)
        delay = None
        if not imm:
            delay = rl_config_for_key.get("max_wait", 10)

        limiter = Limiter(self._bucket_factory, TimeClock(), raise_when_fail=True,
                          max_delay=(delay * 1000) if delay else None)
        self._limiters[bucket_key] = limiter
        return limiter

    def _do_rate_limit(self, message) -> int:
        rl_bucket_key = self._processor.get_rl_bucket_key(message)
        if rl_bucket_key is None:
            return True

        limiter = self._get_limiter(rl_bucket_key)

        while True:
            try:
                limiter.try_acquire(rl_bucket_key)
                return 0
            except (LimiterDelayException, BucketFullException) as e:
                self._logger.debug("Rate limited: " + str(e))
                return SyncKafkaMessageProcessor.ERROR_RATE_LIMITED_IMMEDIATE if limiter.max_delay is None \
                    else SyncKafkaMessageProcessor.ERROR_RATE_LIMITED_WITH_TIMEOUT
            except AssertionError as e:
                # Re-acquire not successful due to a race
                self._logger.warning("Rate limiter assertion error, trying again", exc_info=e)
                sleep(0.1)
                continue
        return 0

    @property
    def rate_limiter_enabled(self):
        return self._bucket_factory is not None

    @staticmethod
    def _serialize(value: dict) -> bytes:
        return json.dumps(value, indent=None, separators=(',', ':')).encode("utf-8")


class AioWorkerProcess(WorkerProcess):
    SLEEP = 0.05

    def __init__(self, worker_id: int, config: dict, processor_type: Type[AsyncKafkaMessageProcessor],
                 to_process: mp.Queue, processed: mp.Queue):
        super().__init__(worker_id, config, processor_type, to_process, processed)

    def run(self):
        import asyncio
        self._logger.info("Starting AIO loop")
        try:
            asyncio.run(self._run_async())
        except KeyboardInterrupt:
            self._logger.info("Interrupted. Shutting down")
            self._running = False

        self._logger.info("Finished (PID %s)", os.getpid())

    def _init_rate_limiting(self):
        redis_pool = redis.asyncio.ConnectionPool.from_url(self._config.get("client", {}).get("redis_uri"))
        self._bucket_factory = _CustomBucketFactory(config=self._config, redis_pool=redis_pool)

    async def _do_rate_limit(self, message) -> int:
        rl_bucket_key = self._processor.get_rl_bucket_key(message)
        if rl_bucket_key is None:
            return True

        limiter = self._get_limiter(rl_bucket_key)
        while True:
            try:
                await limiter.try_acquire(rl_bucket_key)
                return 0
            except (LimiterDelayException, BucketFullException):
                self._logger.debug("Rate limited")
                return SyncKafkaMessageProcessor.ERROR_RATE_LIMITED_IMMEDIATE if limiter.max_delay is None \
                    else SyncKafkaMessageProcessor.ERROR_RATE_LIMITED_WITH_TIMEOUT
            except AssertionError as e:
                # Re-acquire not successful due to a race
                self._logger.warning("Rate limiter assertion error, trying again", exc_info=e)
                await asyncio.sleep(0.1)
                continue
        return 0

    async def _process(self, key_bytes, value_bytes, partition, offset) -> list:
        message = Message(key_bytes, value_bytes, None, None, partition, offset)

        # Deserialize the message using user code
        # The user code should populate the 'key' and 'value' fields of the Message object
        try:
            self._processor.deserialize(message)
        except Exception as e:
            ret = self._processor.process_error(message, e)
            raise _LoopError(ret) from e

        # Rate limiting
        if self.rate_limiter_enabled:
            rl_result = await self._do_rate_limit(message)
            if rl_result != 0:
                ret = self._processor.process_error(message, rl_result)
                raise _LoopError(ret)

        # User processing function
        try:
            ret = await self._processor.process(message)
            self._logger.debug("Processed at p=%s, o=%s", partition, offset)
            return ret
        except Exception as e:
            ret = self._processor.process_error(message, e)
            raise _LoopError(ret) from e

    async def _run_async(self):
        import asyncio

        while self._running:
            partition, offset, key, value = None, None, None, None

            # Try consuming a message
            # Watch for interrupts and errors
            try:
                partition, offset, key, value = self._to_process.get_nowait()
                self._logger.debug("Processing at p=%s, o=%s", partition, offset)
            except queue.Empty:
                try:
                    await asyncio.sleep(self.SLEEP)
                    continue
                except asyncio.CancelledError:
                    self._logger.info("Interrupted. Shutting down")
                    self._running = False
            except KeyboardInterrupt:
                self._logger.info("Interrupted. Shutting down")
                self._running = False
            except Exception as e:
                self._logger.error("Unexpected error. Shutting down", exc_info=e)
                self._running = False

            # A message has been consumed from the input queue
            # We must ensure it gets back to the processed queue
            if partition is not None:
                ret = None
                try:
                    # Though if cancellation was requested, don't actually classify the input
                    if self._running:
                        ret_awaitable = self._process(key, value, partition, offset)
                        assert isawaitable(ret_awaitable)
                        ret = await ret_awaitable
                except (KeyboardInterrupt, asyncio.CancelledError):
                    self._logger.info("Interrupted. Shutting down")
                    self._running = False
                except _LoopError as e:
                    self._logger.debug("Processed with error at p=%s, o=%s", partition, offset,
                                       exc_info=e.__cause__)
                    ret = e.error_messages_to_send
                except Exception as e:
                    self._logger.error("Unexpected error. Shutting down", exc_info=e)
                    self._running = False
                finally:
                    while True:
                        try:
                            self._processed.put((partition, offset, ret or []), False)
                            break
                        except queue.Full:
                            try:
                                await asyncio.sleep(self.SLEEP)
                                continue
                            except asyncio.CancelledError:
                                break

                    self._logger.debug("Placed to processed q at p=%s, o=%s", partition, offset)


def sigterm_handler(signal_num, stack_frame):
    global _process
    if _process is not None:
        _process.close()
        _process = None


def init_process(worker_id: int, config: dict, processor_type: Type[KafkaMessageProcessor],
                 to_process: mp.Queue, processed: mp.Queue, logger_config: dict):
    global _process

    logging.config.dictConfig(logger_config)
    signal.signal(signal.SIGTERM, sigterm_handler)
    if issubclass(processor_type, AsyncKafkaMessageProcessor):
        _process = AioWorkerProcess(worker_id, config, processor_type, to_process, processed)
    else:
        _process = WorkerProcess(worker_id, config, processor_type, to_process, processed)
    _process.run()
