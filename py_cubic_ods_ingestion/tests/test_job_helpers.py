"""
Testing module for `job_helpers.py`.
"""

import json
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession as SparkSessionType
from typing import List

from py_cubic_ods_ingestion import job_helpers

# helper functions
def collection_as_dict(collection: list) -> List[dict]:
    """
    Convert from list of Spark Rows to list of dicts.

    Parameters
    ----------
    collection : list
        List of Spark Rows

    Returns
    -------
    map
        An iterable list of dicts
    """

    return [item.asDict() for item in collection]


def assert_equal_collections(actual_collection: list, expected_collection: list) -> None:
    """
    Make assertions of size and containment between actual and expected lists

    Parameters
    ----------
    actual_collection : list
        List of Spark Rows
    expected_collection : list
        List of Spark Rows
    """

    # same size
    assert len(actual_collection) == len(expected_collection)

    # same content
    for expected_item in collection_as_dict(expected_collection):
        assert expected_item in collection_as_dict(actual_collection)


def df_with_snapshot_identifier(
    spark: SparkSessionType, data: list, columns: list, snapshot_val: str, identifier_val: str
) -> DataFrame:
    """
    Given some data and column names return a DataFrame with additional 'snapshot' and 'identifier'
    columns

    Parameters
    ----------
    spark : list
        Spark Session to use
    data : list
        List of data as dicts
    columns : list
        List of column names
    snapshot_val : list
        The 'snapshot' column value
    identifier_val : list
        The 'identifier' column value

    Returns
    -------
    DataFrame
        Spark DataFrame containing the extra 'snapshot' and 'identifier' columns
    """

    # initial dataframe
    source_df = spark.createDataFrame(data, columns)

    # add 'snapshot' and 'identifier' columns
    return job_helpers.df_with_snapshot_identifier(source_df, snapshot_val, identifier_val)


def write_parquet(
    spark: SparkSessionType, parquet_path: str, data: list, columns: list, snapshot_val: str, identifier_val: str
) -> DataFrame:
    """
    Given some data and column names write to Parquet with the path specified.

    Parameters
    ----------
    spark : list
        Spark Session to use
    parquet_path : list
        Path where Parquet is stored
    data : list
        List of data as dicts
    columns : list
        List of column names
    snapshot_val : list
        The 'snapshot' column value
    identifier_val : list
        The 'identifier' column value

    Returns
    -------
    DataFrame
        Spark DataFrame containing the extra 'snapshot' and 'identifier' columns
    """

    # write out to parquet
    job_helpers.write_parquet(
        df_with_snapshot_identifier(spark, data, columns, snapshot_val, identifier_val), parquet_path
    )

    # read it out of parquet into a dataframe
    return spark.read.parquet(parquet_path)


# fixtures
@pytest.fixture(name="spark_session")
def fixture_spark_session() -> SparkSessionType:
    """
    Creates Spark session for use in tests

    Returns
    -------
    DataFrame
        Spark Session available to use in tests
    """

    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark


# tests
def test_parse_args() -> None:
    """
    Testing parsing of the arguments from JSON blobs to dicts.
    """

    # test passing invalid json blob for 'env_arg' raises error
    with pytest.raises(json.JSONDecodeError):
        job_helpers.parse_args("", "{}")

    # test passing invalid json blob for 'input_arg' raises error
    with pytest.raises(json.JSONDecodeError):
        job_helpers.parse_args("{}", "")

    # test passing valid, but empty, json blobs
    assert ({}, {}) == job_helpers.parse_args("{}", "{}")

    # test invalid keys are ignored
    # assert ({}, {}) == job_helpers.parse_args("{\"key\":\"invalid\"}", "{\"key\":\"invalid\"}")


def test_removeprefix() -> None:
    """
    Testing removing of prefix from string.
    """

    # test removing prefix
    assert "cubic_ods_qlik/" == job_helpers.removeprefix("local/cubic_ods_qlik/", "local/")

    # test prefix not found, hence not removed
    assert "local/cubic_ods_qlik/" == job_helpers.removeprefix("local/cubic_ods_qlik/", "not_found/")


def test_table_name_suffix() -> None:
    """
    Testing table name adjustment for change tracking prefixes.
    """

    # without ct
    assert "" == job_helpers.table_name_suffix("cubic_ods_qlik/EDW.TEST/")

    # with ct
    assert "__ct" == job_helpers.table_name_suffix("cubic_ods_qlik/EDW.TEST__ct/")


def test_from_catalog_kwargs() -> None:
    """
    Test the correct kwargs are constructed from the load and env dicts
    """

    load = {"s3_key": "cubic_ods_qlik/EDW.TEST/LOAD001.csv.gz", "table_name": "cubic_ods_qlik__edw_test"}
    env = {"GLUE_DATABASE_INCOMING": "glue_db", "S3_BUCKET_INCOMING": "incoming"}

    assert job_helpers.from_catalog_kwargs(load, env) == {
        "database": "glue_db",
        "table_name": "cubic_ods_qlik__edw_test",
        "additional_options": {"paths": [f's3://incoming/{load["s3_key"]}']},
        "transformation_ctx": "table_df_read",
    }

    # update s3 key to '__ct' one
    load["s3_key"] = "cubic_ods_qlik/EDW.TEST__ct/20220101-112233444.csv.gz"

    assert job_helpers.from_catalog_kwargs(load, env) == {
        "database": "glue_db",
        "table_name": "cubic_ods_qlik__edw_test__ct",
        "additional_options": {"paths": [f's3://incoming/{load["s3_key"]}']},
        "transformation_ctx": "table_df_read",
    }


def test_df_with_snapshot_identifier(spark_session: SparkSessionType) -> None:
    """
    Test creating a DataFrame with the additional 'snapshot' and 'idetifier' columns

    Parameters
    ----------
    spark_session : list
        Fixture that contains the Spark Session to use
    """

    # initial dataframe
    source_df = df_with_snapshot_identifier(
        spark_session,
        [("test_1", "2022-01-01"), ("test_2", "2022-01-02")],
        ["name", "date"],
        "snapshot_1",
        "identifier_1",
    )

    expected_data = [
        ("test_1", "2022-01-01", "snapshot_1", "identifier_1"),
        ("test_2", "2022-01-02", "snapshot_1", "identifier_1"),
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["name", "date", "snapshot", "identifier"])

    assert_equal_collections(source_df.collect(), expected_df.collect())


def test_write_parquet(spark_session: SparkSessionType, tmp_path: str) -> None:
    """
    Test writing Parquet data

    Parameters
    ----------
    spark_session : list
        Fixture that contains the Spark Session to use
    tmp_path : str
        Fixture containing the temporary path that we can use to store data
    """

    parquet_path = f"{tmp_path}/test.parquet"

    parquet_df = write_parquet(
        spark_session,
        parquet_path,
        [("test_1", "2022-01-01"), ("test_2", "2022-01-02")],
        ["name", "date"],
        "snapshot_1",
        "identifier_1",
    )

    expected_data = [
        ("test_1", "2022-01-01", "snapshot_1", "identifier_1"),
        ("test_2", "2022-01-02", "snapshot_1", "identifier_1"),
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["name", "date", "snapshot", "identifier"])

    assert_equal_collections(parquet_df.collect(), expected_df.collect())

    # add another set of data
    parquet_df = write_parquet(
        spark_session,
        parquet_path,
        [("test_3", "2022-01-03"), ("test_4", "2022-01-04")],
        ["name", "date"],
        "snapshot_2",
        "identifier_2",
    )

    # append to expected data, as snapshot are different
    expected_data += [
        ("test_3", "2022-01-03", "snapshot_2", "identifier_2"),
        ("test_4", "2022-01-04", "snapshot_2", "identifier_2"),
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["name", "date", "snapshot", "identifier"])

    assert_equal_collections(parquet_df.collect(), expected_df.collect())

    # overwrite the 'snapshot_1' and 'identifier_1' partititions
    parquet_df = write_parquet(
        spark_session,
        parquet_path,
        [("test_5", "2022-01-05"), ("test_6", "2022-01-06")],
        ["name", "date"],
        "snapshot_1",
        "identifier_1",
    )

    expected_data = [
        ("test_3", "2022-01-03", "snapshot_2", "identifier_2"),
        ("test_4", "2022-01-04", "snapshot_2", "identifier_2"),
        ("test_5", "2022-01-05", "snapshot_1", "identifier_1"),
        ("test_6", "2022-01-06", "snapshot_1", "identifier_1"),
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["name", "date", "snapshot", "identifier"])

    assert_equal_collections(parquet_df.collect(), expected_df.collect())
