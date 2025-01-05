"""__main__.py: Defines the entrypoint for the configuration manager program. Handles the main loop, i.e. reading
requests from Kafka, invoking the manager and publishing the configuration change results."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import json
import logging
import os
import sys
from json import JSONDecodeError

from confluent_kafka import Consumer, Producer, KafkaError, TopicPartition, OFFSET_BEGINNING

from . import manager, codes
from .models import ConfigurationChangeResult, ConfigurationValidationError
from .util import read_config, make_consumer_settings, make_producer_settings

logger = logging.getLogger(__package__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
err_h = logging.StreamHandler(stream=sys.stderr)
err_h.setLevel(logging.WARN)
logger.addHandler(err_h)

config = read_config()
manager.init(config)

p_settings = make_producer_settings(config)
c_settings = make_consumer_settings(config)


def error_callback(err: KafkaError):
    logger.error(f"Kafka error: {str(err)}")


p_settings["error_cb"] = error_callback
c_settings["error_cb"] = error_callback


def collect_current_setting(c: Consumer) -> list[tuple[str, ConfigurationChangeResult]]:
    last_messages = {}
    logger.info("Reading configuration_states to collect all current settings")

    c.assign([TopicPartition("configuration_states", 0, OFFSET_BEGINNING)])
    try:
        while True:
            msg = c.poll(2)
            if msg is None:
                break
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                break

            try:
                component_id = msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")

                if component_id in last_messages:
                    if msg.offset() > last_messages[component_id][1]:
                        last_messages[component_id] = (value, msg.offset())
                else:
                    last_messages[component_id] = (value, msg.offset())
            except UnicodeDecodeError:
                logger.error("Invalid component ID received (unicode decode error) at offset %s", msg.offset())
                continue
    except Exception as e:
        logger.error("Error while collecting all current settings", exc_info=e)

    c.unassign()
    c.unsubscribe()

    to_configure = {}
    for key, value in last_messages.items():
        conf_str = value[0]
        try:
            conf_dict = json.loads(conf_str)
            to_configure[key] = conf_dict
        except JSONDecodeError as e:
            logger.error("JSON decode error at offset %s: %s", value[1], str(e))
        except Exception as e:
            logger.error("Message processing error at offset %s", value[1], exc_info=e)

    return manager.apply_all_configs(to_configure)


def run():
    logger.info("Starting")
    p = Producer(p_settings, logger=logger)
    c = Consumer(c_settings, logger=logger)
    logger.info("Connected")

    if os.environ.get("SKIP_LOAD") == "1":
        logger.info("Skipping loading of current configuration")
    else:
        snapshots_to_publish = collect_current_setting(c)
        for component_id, snapshot in snapshots_to_publish:
            result_json = snapshot.to_json()
            result_bytes = result_json.encode("utf-8")
            p.produce("configuration_states", key=component_id, value=result_bytes)
        if len(snapshots_to_publish) > 0:
            logger.info("Published %s snapshots", len(snapshots_to_publish))
            p.poll(0)

    logger.info("Subscribing to configuration_change_requests")
    c.subscribe(["configuration_change_requests"])

    try:
        while True:
            msg = c.poll(5)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            try:
                component_id = msg.key().decode("utf-8")
            except UnicodeDecodeError:
                logger.error("Invalid component ID received (unicode decode error) at offset %s", msg.offset())
                continue

            try:
                if not manager.is_known_component(component_id):
                    logger.debug("Unknown component ID received at offset %s: %s", msg.offset(), component_id)
                    continue

                value = msg.value()
                value_dict = json.loads(value)
                result = manager.apply_config(component_id, value_dict)
            except JSONDecodeError as e:
                result = ConfigurationChangeResult(success=False, errors=[
                    ConfigurationValidationError(propertyPath="", errorCode=codes.INVALID_MESSAGE,
                                                 error=str(e))
                ], currentConfig=manager.get_current_config(component_id))
                logger.error("JSON decode error at offset %s: %s", msg.offset(), str(e))
            except Exception as e:
                result = ConfigurationChangeResult(success=False, errors=[
                    ConfigurationValidationError(propertyPath="", errorCode=codes.OTHER,
                                                 error=str(e))
                ], currentConfig=manager.get_current_config(component_id))
                logger.error("Message processing error at offset %s", msg.offset(), exc_info=e)

            if result:
                result_json = result.to_json()
                result_bytes = result_json.encode("utf-8")

                p.produce("configuration_states", key=component_id, value=result_bytes)
                p.poll(0)  # Trigger delivery report
    except KeyboardInterrupt:
        logger.info("Ending")

    c.close()
    p.flush(timeout=5)


if __name__ == "__main__":
    run()
