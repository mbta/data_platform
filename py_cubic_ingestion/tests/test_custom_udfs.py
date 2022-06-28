"""
Tests for our custom UDFs
"""

from py_cubic_ingestion import custom_udfs
import datetime
import pytest


def test_is_empty() -> None:
    assert custom_udfs.is_empty(None)
    assert custom_udfs.is_empty("")
    assert custom_udfs.is_empty("  ")
    assert custom_udfs.is_empty("\t")
    assert custom_udfs.is_empty("\n")
    assert custom_udfs.is_empty("\v")
    assert custom_udfs.is_empty("\f")
    assert custom_udfs.is_empty("\r")
    assert not custom_udfs.is_empty("test")


def test_as_long() -> None:
    # empty
    assert custom_udfs.as_long("") is None

    # minimum
    assert -9223372036854775808 == custom_udfs.as_long("-9223372036854775808")
    # maximum
    assert 9223372036854775807 == custom_udfs.as_long("9223372036854775807")

    # over the limit
    with pytest.raises(ValueError):
        custom_udfs.as_long("9223372036854775808")  # 9223372036854775807 + 1

    # parsing error
    with pytest.raises(ValueError):
        custom_udfs.as_long("invalid")


def test_as_double() -> None:
    # empty
    assert custom_udfs.as_double("") is None

    # negative
    assert -123.45 == custom_udfs.as_double("-123.45")
    # positive
    assert 123.45 == custom_udfs.as_double("123.45")

    # parsing error
    with pytest.raises(ValueError):
        custom_udfs.as_double("invalid")


def test_as_date() -> None:
    # empty
    assert custom_udfs.as_date("") is None

    # typical
    assert datetime.date(2022, 1, 1) == custom_udfs.as_date("2022-01-01")
    # different format
    assert datetime.date(2022, 1, 2) == custom_udfs.as_date("20220102")
    # timestamp
    assert datetime.date(2022, 1, 3) == custom_udfs.as_date("2022-01-03 23:34:56")
    # timestamp with timezome
    assert datetime.date(2022, 1, 3) == custom_udfs.as_date("2022-01-03 23:34:56-04:00")

    # parsing error
    with pytest.raises(ValueError):
        custom_udfs.as_date("invalid")


def test_as_timestamp() -> None:
    # empty
    assert custom_udfs.as_timestamp("") is None

    # typical
    assert datetime.datetime(2022, 1, 1, 12, 34, 56) == custom_udfs.as_timestamp("2022-01-01 12:34:56")
    # with timezone
    assert datetime.datetime(2022, 1, 1, 16, 34, 56) == custom_udfs.as_timestamp("2022-01-01 12:34:56-04:00")
    # different date format
    assert datetime.datetime(2022, 1, 2, 12, 34, 56) == custom_udfs.as_timestamp("20220102 12:34:56")
    # just date
    assert datetime.datetime(2022, 1, 3, 0, 0, 0) == custom_udfs.as_timestamp("2022-01-03")

    # parsing error
    with pytest.raises(ValueError):
        custom_udfs.as_timestamp("invalid")
