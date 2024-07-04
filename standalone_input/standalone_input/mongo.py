"""Mongo wrapper that handles dataset storage"""
__authors__ = ["Adam Horák <ihorak@fit.vut.cz>",
               "Ondřej Ondryáš <xondry02@vut.cz"]

import atexit
import concurrent.futures
from math import ceil
from typing import List, Tuple

import click
import pymongo
import pymongo.errors
from pymongo.cursor import Cursor

from .models import MongoDomainMetadata

import logging

logger = logging.getLogger(__name__)


def chunks(source_list: List, n: int):
    """Yield successive equal about-n-sized chunks from source_list."""
    chunk_count = ceil(len(source_list) / n)
    if chunk_count == 0:
        yield source_list
    else:
        chunk_size = ceil(len(source_list) / chunk_count)
        for i in range(0, len(source_list), chunk_size):
            yield source_list[i:i + chunk_size]


class MongoWrapper:
    batch_queue = []

    def __init__(self, collection: str, config: dict):
        self._config = config = config.get("mongo", {})
        self._client = pymongo.MongoClient(config.get("uri"))
        self._db = self._client[config.get("db")]
        self._collection = self._db[collection]
        self._write_batch_size = config.get("write_batch_size", 100)
        self._read_batch_size = config.get("read_batch_size", 100)

        self._closed = False
        atexit.register(self.cleanup)

    def __del__(self):
        self.cleanup()

    def cleanup(self):
        if not hasattr(self, '_closed') or self._closed:
            return

        if self._write_batch_size > len(self.batch_queue) > 0:
            logger.debug("DB: Flushed remaining %d items before exit", len(self.batch_queue))
        self._flush()
        self._client.close()
        self._closed = True

    def _insert(self, data: List):
        return self._collection.insert_many(data)

    def _upsert(self, data: List, key: str = '_id', skip_duplicates: bool = False):
        updates = [pymongo.UpdateOne({key: d[key]},
                                     {'$setOnInsert' if skip_duplicates else '$set': d},
                                     upsert=True) for d in data]
        return self._collection.bulk_write(updates, ordered=False)

    def _insert_if_not_exists(self, data: List) -> tuple[list, int]:
        """Returns a list of IDs of colliding documents"""
        inserts = [pymongo.InsertOne(d.to_dict()) for d in data]
        try:
            res = self._collection.bulk_write(inserts, ordered=False)
            return [], res.inserted_count
        except pymongo.errors.BulkWriteError as e:
            write_errors = e.details.get('writeErrors', [])
            return [error.get('op').get('_id') for error in write_errors], e.details.get('nInserted', 0)

    def _upsert_one(self, data: dict, key: str):
        return self._collection.update_one({key: data[key]}, {'$set': data}, upsert=True)

    def _flush(self, key: str = '_id', skip_duplicates: bool = False):
        if self.batch_queue:
            self._upsert(self.batch_queue, key, skip_duplicates)
            self.batch_queue.clear()

    def index_by(self, key: str):
        try:
            self._collection.create_index(key, name=f'{key}_index', unique=True)
        except pymongo.errors.OperationFailure:
            pass

    def update_one(self, filter: dict, data: dict):
        return self._collection.update_one(filter, data)

    def flush(self, skip_duplicates: bool = False):
        self._flush(skip_duplicates=skip_duplicates)

    # storing

    def store(self, data: MongoDomainMetadata, skip_duplicates: bool = False):
        # add to batch queue
        self.batch_queue.append(data)
        # flush if batch queue is full
        if len(self.batch_queue) >= self._write_batch_size:
            logger.debug("DB: Batch queue full, flushing " + str(len(self.batch_queue)) + " items")
            self._flush(skip_duplicates=skip_duplicates)

    def bulk_store(self, data: List[MongoDomainMetadata]):
        """Bulk store data, no batch queue"""
        self._upsert(data)

    def parallel_store(self, data: List[MongoDomainMetadata], skip_duplicates: bool = False):
        """Bulk store data in parallel, no batch queue"""
        with concurrent.futures.ThreadPoolExecutor() as executor:
            click.echo(f'Preparing {len(data)} items...')
            futures = [executor.submit(self._upsert, chunk, '_id', skip_duplicates)
                       for chunk in chunks(data, self._write_batch_size)]
            stored = 0
            with click.progressbar(length=len(futures), show_pos=True, show_percent=True, label="Writes") as loading:
                for future in concurrent.futures.as_completed(futures):
                    loading.update(1)
                    stored += future.result().upserted_count
            result = f'Stored {stored} of {len(data)} items in {len(futures)} writes'
            logger.info(result)
            click.echo(result)
        return stored, len(futures)

    def parallel_insert_new(self, data: List[MongoDomainMetadata]):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            existing_ids = []
            click.echo(f'Preparing {len(data)} items...')
            futures = [executor.submit(self._insert_if_not_exists, chunk)
                       for chunk in chunks(data, self._write_batch_size)]
            stored = 0
            with click.progressbar(length=len(futures), show_pos=True, show_percent=True, label="Writes") as loading:
                for future in concurrent.futures.as_completed(futures):
                    loading.update(1)
                    existing, count = future.result()
                    existing_ids = existing_ids + existing
                    stored += count
            result = f'Inserted {stored} out of {len(data)} items in {len(futures)} writes'
            logger.info(result)
            click.echo(result)
        return stored, len(futures), existing_ids

    # retrieving
    def _find_query(self, query, limit: int = 0) -> Tuple[Cursor[MongoDomainMetadata], int]:
        db_count = self._collection.count_documents(query)
        count = db_count if limit == 0 else min(limit, db_count)
        return self._collection.find(query, limit=limit, batch_size=self._read_batch_size), count
