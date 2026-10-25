"""
The batching settings, read from the arguments and the OTEL_BSP_* variables and checked as `BatchSpanProcessor`
reads and checks them, with the same defaults, messages and error logs (on the `fastotel` logger).
"""

import logging
import os

OTEL_BSP_MAX_QUEUE_SIZE = "OTEL_BSP_MAX_QUEUE_SIZE"
OTEL_BSP_SCHEDULE_DELAY = "OTEL_BSP_SCHEDULE_DELAY"
OTEL_BSP_MAX_EXPORT_BATCH_SIZE = "OTEL_BSP_MAX_EXPORT_BATCH_SIZE"
OTEL_BSP_EXPORT_TIMEOUT = "OTEL_BSP_EXPORT_TIMEOUT"
_DEFAULT_MAX_QUEUE_SIZE = 2048
_DEFAULT_SCHEDULE_DELAY_MILLIS = 5000
_DEFAULT_MAX_EXPORT_BATCH_SIZE = 512
_DEFAULT_EXPORT_TIMEOUT_MILLIS = 30000

logger = logging.getLogger("fastotel")


def max_queue_size(argument: int | None) -> int:
    if argument is None:
        return _from_environ(OTEL_BSP_MAX_QUEUE_SIZE, _DEFAULT_MAX_QUEUE_SIZE)
    return argument


def schedule_delay_millis(argument: float | None) -> float:
    if argument is None:
        return _from_environ(OTEL_BSP_SCHEDULE_DELAY, _DEFAULT_SCHEDULE_DELAY_MILLIS)
    return argument


def max_export_batch_size(argument: int | None) -> int:
    if argument is None:
        return _from_environ(OTEL_BSP_MAX_EXPORT_BATCH_SIZE, _DEFAULT_MAX_EXPORT_BATCH_SIZE)
    return argument


def export_timeout_millis(argument: float | None) -> float:
    if argument is None:
        return _from_environ(OTEL_BSP_EXPORT_TIMEOUT, _DEFAULT_EXPORT_TIMEOUT_MILLIS)
    return argument


def check(  # noqa: WPS238 - the four checks of BatchSpanProcessor, in its order
    max_queue_size: int,
    schedule_delay_millis: float,
    max_export_batch_size: int,
) -> None:
    # The checks and messages of BatchSpanProcessor
    if max_queue_size <= 0:
        raise ValueError("max_queue_size must be a positive integer.")
    # `not > 0` also refuses NaN, which the SDK lets through to spin its worker
    if not schedule_delay_millis > 0:
        raise ValueError("schedule_delay_millis must be positive.")
    if max_export_batch_size <= 0:
        raise ValueError("max_export_batch_size must be a positive integer.")
    if max_export_batch_size > max_queue_size:
        raise ValueError("max_export_batch_size must be less than or equal to max_queue_size.")


def _from_environ(name: str, default: int) -> int:
    # As BatchSpanProcessor reads its variables: int() of the value, or the default and the same error log
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        logger.exception("Unable to parse value for %s as integer. Defaulting to %s.", name, default)
        return default
