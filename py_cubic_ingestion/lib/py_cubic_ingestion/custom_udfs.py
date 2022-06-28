"""
Custome Spark user-defined functions (UDFs) for processing dataframes.
"""

from datetime import date, datetime
from typing import Optional
from dateutil import parser
from dateutil.tz import UTC


def is_empty(s: Optional[str]) -> bool:
    return not (s and not s.isspace())


def as_long(s: str) -> Optional[int]:
    try:
        if is_empty(s):
            return None

        # int is the type for long as well
        val = int(s)
        if val < -9223372036854775808 or val > 9223372036854775807:
            raise ValueError(f"'{s}', Out of range of Spark LongType data type. Use DecimalType.")

        return val
    except ValueError as e:
        raise ValueError(f"'{s}', {e}") from e


def as_double(s: str) -> Optional[float]:
    try:
        return float(s) if not is_empty(s) else None
    except ValueError as e:
        raise ValueError(f"'{s}', {e}") from e


def as_date(s: str) -> Optional[date]:
    try:
        return parser.isoparse(s).date() if not is_empty(s) else None
    except ValueError as e:
        raise ValueError(f"'{s}', {e}") from e


def as_timestamp(s: str) -> Optional[datetime]:
    try:
        if is_empty(s):
            return None

        val = parser.isoparse(s)
        if val.tzinfo:
            val = val.astimezone(UTC).replace(tzinfo=None)

        return val
    except ValueError as e:
        raise ValueError(f"'{s}', {e}") from e
