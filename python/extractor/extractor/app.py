"""app.py: The main module for the feature extractor component. Defines the Faust application."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import io
import time
from json import loads, JSONDecodeError

from common import log
from domrad_kafka_client import SyncKafkaMessageProcessor, Message, SimpleMessage
from . import extractor

EXTRACTOR = "extractor"
COMPONENT_NAME = EXTRACTOR


class ExtractorProcessor(SyncKafkaMessageProcessor[str, dict]):

    def __init__(self, config: dict):
        super().__init__(config)
        self._logger = log.init(COMPONENT_NAME)

        component_config = config.get(EXTRACTOR, {})
        self._produce_jsons = component_config.get("produce_jsons", False)
        self._produce_dfs = not component_config.get("only_produce_jsons", False)
        self._batch_size = component_config.get("batch_size", 50)
        self._batch_timeout = component_config.get("batch_timeout", 5)
        self._batch_enabled = self._produce_dfs and self._batch_size > 1 and self._batch_timeout > 0

        self._output_topic_df = 'feature_vectors'
        self._output_topic_json = 'feature_vectors_json'

        self._buffer = io.BytesIO()
        self._current_batch_dfs = []
        self._last_batch_send_time = time.time()

        if not self._produce_jsons and not self._produce_dfs:
            self._logger.error(
                "The 'only_produce_jsons' option is set, but 'produce_jsons' is not. Refusing to operate.")
            exit(1)

        extractor.init_transformations(component_config)

    def deserialize(self, message: Message[str, dict]) -> None:
        msg_key = message.key_raw.decode('utf-8')  # type: str
        value_bytes = message.value_raw  # type: bytes

        message.key = msg_key
        if value_bytes is None:
            # Empty entries will be ignored
            return None
        try:
            value = loads(value_bytes)
            value["domain_name"] = msg_key
            value["invalid_data"] = False
            message.value = value
        except JSONDecodeError:
            message.value = {"domain_name": msg_key, "invalid_data": True}

    def _serialize_df(self, df) -> bytes:
        buffer = self._buffer
        # Reset the output buffer position
        buffer.seek(0)
        # Serialize the dataframe into a memory buffer
        # noinspection PyTypeChecker
        df.to_feather(buffer)
        # Get the result bytes
        return buffer.getbuffer()[0:buffer.tell()].tobytes()

    def process(self, message: Message[str, dict]) -> list[SimpleMessage]:
        logger = self._logger

        df, errors = extractor.extract_features([message.value])
        ret = []

        if df is not None:
            # JSONs ignore batching
            if self._produce_jsons:
                # Get rid of the only Timedelta object in the vector (temporary, will be addressed globally)
                df["rdap_registration_period"] = df["rdap_registration_period"].dt.total_seconds() / 60

                # Serialize the dataframe into a JSON array and produce the vectors as individual messages
                for _, row in df.iterrows():
                    row_json = row.to_json(orient='index')
                    ret.append((self._output_topic_json, row["domain_name"].encode("utf-8"), row_json.encode("utf-8")))

            if self._produce_dfs:
                if self._batch_enabled:
                    self._current_batch_dfs.append(df)
                    if len(self._current_batch_dfs) >= self._batch_size or \
                            time.time() - self._last_batch_send_time >= self._batch_timeout:
                        if len(self._current_batch_dfs) > 1:
                            # Concatenate all the dataframes in the batch
                            df = df.concat(self._current_batch_dfs, ignore_index=True)
                        # Reset the batch
                        self._current_batch_dfs.clear()
                        self._last_batch_send_time = time.time()
                        # Send the result
                        ret.append((self._output_topic_df, None, self._serialize_df(df)))
                else:
                    ret.append((self._output_topic_df, None, self._serialize_df(df)))

        if len(errors) > 0:
            for key, error in errors.items():
                logger.k_unhandled_error(error, key)

        return ret

    def process_error(self, message: Message[str, dict], error: BaseException | int) -> list[SimpleMessage]:
        if isinstance(error, BaseException):
            self._logger.k_unhandled_error(error, message.key)
        elif error == self.ERROR_RATE_LIMITED_IMMEDIATE or error == self.ERROR_RATE_LIMITED_WITH_TIMEOUT:
            self._logger.k_info("Rate limited", message.key)
        else:
            self._logger.k_unhandled_error(ValueError(), message.key, error_id=error)

        return []
