"""
Custome Spark user-defined functions (UDFs) for processing dataframes.
"""

from datetime import date, datetime
from dateutil import parser
from dateutil.tz import UTC
from typing import Callable, Optional, TypeVar
import functools


T = TypeVar("T")


def optional(as_type: Callable[[str], T]) -> Callable[[Optional[str]], Optional[T]]:
    @functools.wraps(as_type)
    def wrapper(s: Optional[str]) -> Optional[T]:
        return as_type(s) if s and not s.isspace() else None

    return wrapper


def capture_error(as_type: Callable[[str], T]) -> Callable[[str], T]:
    @functools.wraps(as_type)
    def wrapper(s: str) -> T:
        try:
            return as_type(s)
        except ValueError as e:
            raise ValueError(f"'{s}', {e}") from e

    return wrapper


@optional
@capture_error
def as_long(s: str) -> int:
    # int is the type for long as well
    val = int(s)
    if val < -9223372036854775808 or val > 9223372036854775807:
        raise ValueError("Out of range of Spark LongType data type.")

    return val


@optional
@capture_error
def as_double(s: str) -> float:
    return float(s)


@optional
@capture_error
def as_date(s: str) -> date:
    return parser.isoparse(s).date()


@optional
@capture_error
def as_timestamp(s: str) -> datetime:
    val = parser.isoparse(s)
    # if we have picked up a timezone, then localize to UTC and drop it
    if val.tzinfo:
        val = val.astimezone(UTC).replace(tzinfo=None)

    return val
