import logging
import typing
from time import sleep
from .worker_manager import WorkerManager
from .message_processor import KafkaMessageProcessor
from . import util
import confluent_kafka as ck


class KafkaClient:
    def __init__(self, config: dict, input_topic: str, processor_type: typing.Type[KafkaMessageProcessor]):
        self._config = config
        self._input_topic = input_topic
        self._processor_type = processor_type

        self._consumer: ck.Consumer = typing.cast(ck.Consumer, None)
        self._producer: ck.Producer = typing.cast(ck.Producer, None)
        self._request_handler: WorkerManager = typing.cast(WorkerManager, None)
        self._running = False
        self._client_config = config.get("client", {})
        self._logger = logging.getLogger("main")

    def run(self):
        consumer_settings = util.make_consumer_settings(self._config)
        producer_settings = util.make_producer_settings(self._config)
        producer_settings["on_delivery"] = self._delivery_callback

        self._logger.info("Initializing consumer")
        # noinspection PyArgumentList
        self._consumer = ck.Consumer(consumer_settings, logger=self._logger)
        self._logger.info("Consumer initialized")

        self._logger.info("Initializing producer")
        # noinspection PyArgumentList
        self._producer = ck.Producer(producer_settings, logger=self._logger)
        self._producer.init_transactions()
        self._logger.info("Producer initialized")

        self._request_handler = WorkerManager(self._config, self._input_topic, self._processor_type,
                                              self._consumer, self._producer)
        sleep(self._client_config.get("init_wait", 0))
        self._run_consume_loop()

    def _run_consume_loop(self):
        self._running = True
        timeout = self._client_config.get("poll_timeout", 1.0)

        try:
            self._logger.info("Subscribing to %s", self._input_topic)
            self._consumer.subscribe([self._input_topic])

            while self._running:
                try:
                    # Serve delivery reports from previous produces
                    self._producer.poll(0)

                    # Process and produce results from workers
                    self._request_handler.handle_results()

                    # Check the queue size
                    if not self._request_handler.can_continue():
                        sleep(0.5)
                        continue

                    # Wait for a new message to come
                    msg = self._consumer.poll(timeout=timeout)
                    if msg is None:
                        continue

                    if msg.error():
                        # TODO: Handle error
                        self._logger.warning("Consumer error", exc_info=ck.KafkaException(msg.error()))
                        continue

                    # Pass the message to the handler
                    self._request_handler.handle_message(msg)
                except KeyboardInterrupt:
                    self._logger.info("Ending")
                    self._running = False
                    break
                except Exception as e:
                    self._logger.error("Unexpected consumption error", exc_info=e)
                    self._running = False
                    break
        finally:
            self._request_handler.close()
            # Close down the consumer to commit final offsets
            self._logger.info("Closing the consumer")
            self._consumer.close()

    def _delivery_callback(self, err, msg):
        if err:
            key = msg.key()
            self._logger.warning("Result '%s' failed delivery: %s", key, err)
