"""
Custome Spark user-defined functions (UDFs) for processing dataframes.
"""

from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType, DoubleType, LongType, TimestampType
from typing import Optional
from dateutil import parser


def is_empty(s: Optional[str]) -> bool:
    return not (s and not s.isspace())


@udf(returnType=LongType())
def as_long(s: str) -> Optional[int]:
    try:
        if is_empty(s):
            return None

        # int is the type for long as well
        val = int(s)
        if -9223372036854775807 > val > 9223372036854775807:
            raise ValueError("Out of range of Spark LongType data type.")

        return val
    except ValueError as e:
        raise ValueError(f"'{s}', {e}") from e


@udf(returnType=DoubleType())
def as_double(s: str) -> Optional[float]:
    try:
        if is_empty(s):
            return None

        # float is the type for double
        val = float(s)
        if -9223372036854775807.9223372036854775807 > val > 9223372036854775807.9223372036854775807:
            raise ValueError("Out of range of Spark DoubleType data type.")

        return val
    except ValueError as e:
        raise ValueError(f"'{s}', {e}") from e


@udf(returnType=DateType())
def as_date(s: str) -> Optional[datetime]:
    try:
        return parser.isoparse(s) if not is_empty(s) else None
    except ValueError as e:
        raise ValueError(f"'{s}', {e}") from e


@udf(returnType=TimestampType())
def as_timestamp(s: str) -> Optional[datetime]:
    try:
        # float is the type for double
        return parser.isoparse(s) if not is_empty(s) else None
    except ValueError as e:
        raise ValueError(f"'{s}', {e}") from e
