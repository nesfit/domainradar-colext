import logging

import pytest


@pytest.fixture(autouse=True)
def ensure_trace_logger():
    if not hasattr(logging.Logger, "trace"):
        logging.Logger.trace = logging.Logger.debug
    yield
