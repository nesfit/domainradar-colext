import os
import uuid

import pytest
import redis
from pyrate_limiter import BucketFullException, Limiter, LimiterDelayException, TimeClock

from kafka_multiprocessor.worker_process import _CustomBucketFactory


@pytest.mark.integration
def test_redis_rate_limiter_enforces_limits():
    redis_uri = os.getenv("REDIS_URI")
    if not redis_uri:
        pytest.skip("REDIS_URI not set")

    bucket_key = f"test-{uuid.uuid4()}"
    config = {
        "rate_limiter": {
            "default": {
                "immediate": True,
                "rates": [
                    {"requests": 1, "interval_ms": 10_000},
                ],
            }
        }
    }

    pool = redis.ConnectionPool.from_url(redis_uri)
    factory = _CustomBucketFactory(config=config, redis_pool=pool)
    limiter = Limiter(factory, TimeClock(), raise_when_fail=True, max_delay=None)

    limiter.try_acquire(bucket_key)
    with pytest.raises((BucketFullException, LimiterDelayException)):
        limiter.try_acquire(bucket_key)
