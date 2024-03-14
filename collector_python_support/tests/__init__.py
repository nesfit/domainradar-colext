import logging
import pytest


@pytest.fixture(scope='session')
def logger():
    return logging.getLogger("test_logger")
