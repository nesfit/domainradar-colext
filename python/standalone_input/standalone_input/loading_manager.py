from typing import Iterable

import click
import logging

from confluent_kafka import KafkaError, Producer

from .loaders import TLoader
from .models import MongoDomainMetadata
from .mongo import MongoWrapper
from .util import make_producer_settings

logger = logging.getLogger(__name__)


class LoadingManager:
    @staticmethod
    def error_callback(err: KafkaError):
        logger.error(f'Kafka error: {err}')

    def __init__(self, mongo: MongoWrapper | None, config: dict):
        self.mongo = mongo

        producer_settings = make_producer_settings(config)
        producer_settings["error_cb"] = self.error_callback

        logger.info("Starting Kafka producer")
        # noinspection PyArgumentList
        self.producer = Producer(producer_settings, logger=logger)
        logger.info("Connected")

    def _load_without_mongo(self, loader: TLoader):
        total = 0
        click.echo(f'Feeding domains to Kafka.')
        for domain_list in loader.load():
            total += len(domain_list)
            for domain in domain_list:
                self.producer.produce('to_process_zone', key=domain.name.encode('utf-8'))
                self.producer.poll(0)
        self.producer.flush(5)
        click.echo(f'Sent {total} domains to Kafka.')

    def load_from_loader(self, loader: TLoader, force: bool = False):
        if not self.mongo:
            self._load_without_mongo(loader)
            return

        total_existing = 0
        total_stored = 0
        total_writes = 0

        all_to_process = set()

        for domain_list in loader.load():
            all_to_process.update(x.name for x in domain_list)
            stored, writes, existing = self.mongo.parallel_insert_new([MongoDomainMetadata.from_domain(domain)
                                                                       for domain in domain_list])
            total_stored += stored
            total_writes += writes
            total_existing += len(existing)

            if not force:
                all_to_process.difference_update(existing)

        result = f"Inserted {total_stored} domains in {total_writes} writes, {total_existing} were duplicates."
        click.echo(result)

        if len(all_to_process) == 0:
            click.echo("No new domains to process")
            return

        click.echo(f'Sending {len(all_to_process)} domains to Kafka.')
        for domain_name in all_to_process:
            self.producer.produce('to_process_zone', key=domain_name.encode('utf-8'))
            self.producer.poll(0)
        self.producer.flush(5)
        click.echo('All domains sent.')
