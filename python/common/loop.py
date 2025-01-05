"""loop.py: A lightweight wrapper over aiokafka that provides a Faust-compatible interface. Highly experimental
and currently not fast enough, as it always processes messages one by one."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import asyncio
import json
import logging
from asyncio import CancelledError
from functools import wraps

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from pydantic import BaseModel


class FaustLikeApp:
    class TopicShim:
        INPUT_STR = 1
        INPUT_BYTES = 2
        INPUT_OBJ = 3

        @staticmethod
        def _encode_str(value):
            if value is None:
                return None
            return value.encode('utf-8')

        @staticmethod
        def _encode_bytes(value):
            return value

        @staticmethod
        def _encode_other(value):
            if value is None:
                return None
            elif isinstance(value, BaseModel):
                return value.model_dump_json(indent=None, by_alias=True, context={"separators": (',', ':')}) \
                    .encode("utf-8")
            else:
                return json.dumps(value, indent=None, separators=(',', ':')).encode("utf-8")

        @staticmethod
        def _decode_str(value):
            if value is None:
                return None
            return value.decode('utf-8')

        @staticmethod
        def _decode_bytes(value):
            return value

        @staticmethod
        def _decode_other(value):
            if value is None or len(value) == 0:
                return None
            return json.loads(value.decode("utf-8"))

        def __init__(self, topic: str, key_type: int, val_type: int, faust_like_app):
            self.faust_like_app = faust_like_app
            self.topic = topic
            self.key_encoder = self._encode_str if key_type == self.INPUT_STR else \
                self._encode_bytes if key_type == self.INPUT_BYTES else self._encode_other
            self.val_encoder = self._encode_str if val_type == self.INPUT_STR else \
                self._encode_bytes if val_type == self.INPUT_BYTES else self._encode_other
            self.key_decoder = self._decode_str if key_type == self.INPUT_STR else \
                self._decode_bytes if key_type == self.INPUT_BYTES else self._decode_other
            self.val_decoder = self._decode_str if val_type == self.INPUT_STR else \
                self._decode_bytes if val_type == self.INPUT_BYTES else self._decode_other

        async def send(self, key, value):
            producer = self.faust_like_app.producer
            await producer.send_and_wait(topic=self.topic, key=self.key_encoder(key),
                                         value=self.val_encoder(value))

    class ConsumerShim:
        def __init__(self, consumer, topic_shim, logger: logging.Logger):
            self.logger = logger
            self.consumer = consumer
            self.key_decoder = topic_shim.key_decoder
            self.val_decoder = topic_shim.val_decoder

        async def items(self):
            async for msg in self.consumer:
                try:
                    yield self.key_decoder(msg.key), self.val_decoder(msg.value)
                except (CancelledError, KeyboardInterrupt):
                    break
                except Exception as e:
                    self.logger.error("Error decoding message", exc_info=e)

    def __init__(self, app_id: str, brokers: str | list[str], debug: bool, security_protocol, ssl_context,
                 logger: logging.Logger = None, producer_args=None, consumer_args=None):
        self.debug = debug
        self.ssl_context = ssl_context
        self.security_protocol = security_protocol
        self.app_id = app_id
        self.brokers = brokers
        self.producer = None
        self.producer_args = producer_args
        self.consumer_args = consumer_args
        self._agent = None
        self._agent_topic = None
        self.logger = logger or logging.getLogger(app_id)

    def topic(self, name: str, **kwargs):
        key_type = self._get_encoder_type("key", kwargs)
        val_type = self._get_encoder_type("value", kwargs)

        return self.TopicShim(name, key_type, val_type, self)

    async def _run(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=self.brokers, client_id=self.app_id + "-producer", acks=1,
                                         compression_type='zstd', security_protocol=self.security_protocol,
                                         ssl_context=self.ssl_context, **(self.producer_args or {}))

        consumer = AIOKafkaConsumer(self._agent_topic.topic,
                                    bootstrap_servers=self.brokers, group_id=self.app_id,
                                    security_protocol=self.security_protocol, ssl_context=self.ssl_context,
                                    **(self.consumer_args or {}))

        self.logger.info("Starting producer and consumer")
        await self.producer.start()
        await consumer.start()
        try:
            self.logger.info("Starting the agent")
            await self._agent(self.ConsumerShim(consumer, self._agent_topic, self.logger))
        except (CancelledError, KeyboardInterrupt):
            self.logger.info("Ending")
        except Exception as e:
            self.logger.error("Error in agent", exc_info=e)
        finally:
            await consumer.stop()
            await self.producer.stop()
            self.logger.info("Stopped producer and consumer")

    def agent(self, topic: TopicShim, *args, **kwargs):
        def decorator(fun):
            self._agent = fun
            self._agent_topic = topic

            @wraps(fun)
            async def wrapper():
                return None

            return wrapper

        return decorator

    def main(self):
        coro = self._run()
        asyncio.run(coro, debug=self.debug)

    @staticmethod
    def _get_encoder_type(target, args):
        if args.get(f"{target}_type") is str or args.get(f"{target}_serializer") == "str":
            return FaustLikeApp.TopicShim.INPUT_STR
        elif args.get(f"{target}_type") is bytes or args.get(f"{target}_serializer") == "raw":
            return FaustLikeApp.TopicShim.INPUT_BYTES
        else:
            return FaustLikeApp.TopicShim.INPUT_OBJ
