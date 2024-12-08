import logging
import typing
from time import sleep

from classifier_unit.request_handler import RequestHandler

from . import util
import confluent_kafka as ck

TOPIC_INPUT = "feature_vectors"
TOPIC_OUTPUT = "classification_results"


class ClassifierPipelineClient:
    def __init__(self, config: dict):
        self._consumer: ck.Consumer = typing.cast(ck.Consumer, None)
        self._running = False

        self._config = config
        self._client_config = config.get("client", {})
        self._logger = logging.getLogger(__name__)
        self._request_handler = RequestHandler(config, TOPIC_INPUT, TOPIC_OUTPUT)

    def run(self):
        consumer_settings = util.make_consumer_settings(self._config)

        self._logger.info("Initializing consumer")
        # noinspection PyArgumentList
        self._consumer = ck.Consumer(consumer_settings, logger=self._logger)
        self._logger.info("Consumer connected")

        sleep(self._client_config.get("init_wait", 0))
        self._run_consume_loop()

    def _run_consume_loop(self):
        self._running = True
        timeout = self._client_config.get("poll_timeout", 1.0)

        try:
            self._logger.info("Subscribing to %s", TOPIC_INPUT)
            self._consumer.subscribe([TOPIC_INPUT])
            while self._running:
                try:
                    # Take the newest message from the ROB for which all the previous messages have also finished
                    # and store its offset (the consumer will later commit them)
                    to_confirm = self._request_handler.get_messages_to_confirm()
                    if to_confirm is not None:
                        self._consumer.store_offsets(offsets=to_confirm)

                    if not self._request_handler.can_continue():
                        sleep(0.2)
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
                except Exception as e:
                    # TODO: Handle error
                    self._logger.error("Unexpected consumption error", exc_info=e)
        finally:
            to_confirm = self._request_handler.close()
            if to_confirm is not None:
                self._consumer.store_offsets(offsets=to_confirm)

            # Close down the consumer to commit final offsets
            self._consumer.close()
