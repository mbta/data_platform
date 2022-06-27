"""
Tests for our custom UDFs
"""

from py_cubic_ingestion import custom_udfs
from pyspark.sql import Row
from pyspark.sql.session import SparkSession as SparkSessionType
import datetime


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


def test_as_long(spark_session: SparkSessionType) -> None:
    original_data = [
        (
            # https://spark.apache.org/docs/latest/sql-ref-datatypes.html
            "-9223372036854775807",
            "9223372036854775807",
        )
    ]
    original_df = spark_session.createDataFrame(
        original_data,
        [
            "bigint_col_min",
            "bigint_col_max",
        ],
    )

    updated_df = original_df.select(
        custom_udfs.as_long("bigint_col_min").alias("bigint_col_min"),
        custom_udfs.as_long("bigint_col_max").alias("bigint_col_max"),
    )

    assert updated_df.first() == Row(bigint_col_min=-9223372036854775807, bigint_col_max=9223372036854775807)


def test_as_double(spark_session: SparkSessionType) -> None:
    original_data = [
        (
            # https://spark.apache.org/docs/latest/sql-ref-datatypes.html
            "-9223372036854775807.9223372036854775807",
            "9223372036854775807.9223372036854775807",
        )
    ]
    original_df = spark_session.createDataFrame(
        original_data,
        [
            "double_col_min",
            "double_col_max",
        ],
    )

    updated_df = original_df.select(
        custom_udfs.as_double("double_col_min").alias("double_col_min"),
        custom_udfs.as_double("double_col_max").alias("double_col_max"),
    )

    assert updated_df.first() == Row(
        double_col_min=-9223372036854775807.9223372036854775807, double_col_max=9223372036854775807.9223372036854775807
    )


def test_as_date(spark_session: SparkSessionType) -> None:
    original_data = [
        (
            "2022-01-01",
            "20220102",
        )
    ]
    original_df = spark_session.createDataFrame(
        original_data,
        [
            "date_col",
            "date_col_alt",
        ],
    )

    updated_df = original_df.select(
        custom_udfs.as_date("date_col").alias("date_col"), custom_udfs.as_date("date_col_alt").alias("date_col_alt")
    )

    assert updated_df.first() == Row(date_col=datetime.date(2022, 1, 1), date_col_alt=datetime.date(2022, 1, 2))


def test_as_timestamp(spark_session: SparkSessionType) -> None:
    original_data = [
        (
            "2022-01-01 12:34:56",
            "20220102 12:34:56",
        )
    ]
    original_df = spark_session.createDataFrame(
        original_data,
        [
            "timestamp_col",
            "timestamp_col_alt",
        ],
    )

    updated_df = original_df.select(
        custom_udfs.as_timestamp("timestamp_col").alias("timestamp_col"),
        custom_udfs.as_timestamp("timestamp_col_alt").alias("timestamp_col_alt"),
    )

    assert updated_df.first() == Row(
        timestamp_col=datetime.datetime(2022, 1, 1, 12, 34, 56),
        timestamp_col_alt=datetime.datetime(2022, 1, 2, 12, 34, 56),
    )
