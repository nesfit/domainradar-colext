import logging

from common import log


def test_init_adds_custom_methods():
    logger = log.init("test-logger")

    assert hasattr(logger, "k_debug")
    assert hasattr(logger, "k_info")
    assert hasattr(logger, "k_warning")
    assert hasattr(logger, "k_unhandled_error")
    assert hasattr(logger, "k_trace")


def test_get_returns_logger_with_custom_methods():
    logger = log.get("another-logger")

    assert hasattr(logger, "k_unhandled_error")


def test_extra_formatter_adds_event_key_and_properties():
    formatter = log.ExtraFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )
    record.properties = {"a": 1}

    formatted = formatter.format(record)

    assert "no key" in formatted
    assert "hello" in formatted
    assert "Properties:" in formatted
