"""
Testing module for `job_helpers.py`.
"""

import json
import pytest
import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession as SparkSessionType
from typing import List
from botocore.stub import Stubber
from typing import Iterator

from py_cubic_ingestion import job_helpers

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


def stub_glue_get_table(glue_client: Stubber, table_name: str) -> None:
    """
    Adds a response to the Stubber for Glue's `get_table` call

    Parameters
    ----------
    glue_client : Stubber
        Stubber for Glue client that the response is added for
    table_name : str
        Name of table that Glue Client's stub should return
    """

    glue_client.add_response(
        "get_table",
        expected_params={"DatabaseName": "db", "Name": table_name},
        service_response={
            "Table": {
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "string_col", "Type": "string"},
                        {"Name": "bigint_col", "Type": "bigint"},
                        {"Name": "double_col", "Type": "double"},
                        {"Name": "date_col", "Type": "date"},
                        {"Name": "timestamp_col", "Type": "timestamp"},
                        {"Name": "other_col", "Type": "array"},
                    ]
                },
            }
        },
    )


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


def df_with_partition_columns(spark: SparkSessionType, data: list, columns: list, partition_columns: list) -> DataFrame:
    """
    Given some data and column names return a DataFrame with additional partition
    columns

    Parameters
    ----------
    spark : list
        Spark Session to use
    data : list
        List of data as dicts
    columns : list
        List of column names
    partition_columns : list
        List of dicts with partition information

    Returns
    -------
    DataFrame
        Spark DataFrame containing the extra 'identifier' column
    """

    # initial dataframe
    source_df = spark.createDataFrame(data, columns)

    # add 'identifier' column
    return job_helpers.df_with_partition_columns(source_df, partition_columns)


def write_parquet(
    spark: SparkSessionType, parquet_path: str, data: list, columns: list, partition_columns: list
) -> DataFrame:
    """
    Given some data and column names write to Parquet with the path specified.

    Parameters
    ----------
    spark : list
        Spark Session to use
    parquet_path : str
        Path where Parquet is stored
    data : list
        List of data as dicts
    columns : list
        List of column names
    partition_columns : list
        List of dicts with partition information

    Returns
    -------
    DataFrame
        Spark DataFrame containing the extra 'identifier' column
    """

    # initial dataframe
    source_df = spark.createDataFrame(data, columns)

    # write out to parquet
    job_helpers.write_parquet(
        source_df,
        partition_columns,
        parquet_path,
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


@pytest.fixture(name="glue_client")
def fixture_glue_client() -> Iterator[Stubber]:
    """
    Override Glue client with a Stubber
    """

    with Stubber(job_helpers.glue_client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


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


def test_table_name_suffix() -> None:
    """
    Testing table name adjustment for change tracking prefixes.
    """

    # without ct
    assert "" == job_helpers.table_name_suffix("cubic/ods_qlik/EDW.TEST/")

    # with ct
    assert "__ct" == job_helpers.table_name_suffix("cubic/ods_qlik/EDW.TEST__ct/")


def test_get_glue_table_schema_fields_by_load(glue_client: Stubber) -> None:
    """
    Testing that we are able to get a glue table schema and convert the
    list of fields to the correct types for spark.
    """

    expected_schema_fields = [
        {"name": "string_col", "type": "string"},
        {"name": "bigint_col", "type": "long"},
        {"name": "double_col", "type": "double"},
        {"name": "date_col", "type": "date"},
        {"name": "timestamp_col", "type": "timestamp"},
        {"name": "other_col", "type": "string"},
    ]

    stub_glue_get_table(glue_client, "cubic_ods_qlik__edw_test")

    assert expected_schema_fields == job_helpers.get_glue_table_schema_fields_by_load(
        "db", {"s3_key": "cubic/ods_qlik/EDW.TEST/LOAD001.csv.gz", "table_name": "cubic_ods_qlik__edw_test"}
    )

    # also check '__ct' loads
    stub_glue_get_table(glue_client, "cubic_ods_qlik__edw_test__ct")

    assert expected_schema_fields == job_helpers.get_glue_table_schema_fields_by_load(
        "db",
        {"s3_key": "cubic/ods_qlik/EDW.TEST__ct/20220101-112233444.csv.gz", "table_name": "cubic_ods_qlik__edw_test"},
    )


def test_from_catalog_kwargs() -> None:
    """
    Test the correct kwargs are constructed from the load and env dicts
    """

    load = {"s3_key": "cubic/ods_qlik/EDW.TEST/LOAD001.csv.gz", "table_name": "cubic_ods_qlik__edw_test"}
    env = {"GLUE_DATABASE_INCOMING": "glue_db", "S3_BUCKET_INCOMING": "incoming"}

    assert job_helpers.from_catalog_kwargs(load, env) == {
        "database": "glue_db",
        "table_name": "cubic_ods_qlik__edw_test",
        "additional_options": {"paths": [f's3://incoming/{load["s3_key"]}']},
        "transformation_ctx": "table_df_read",
    }

    # update s3 key to '__ct' one
    load["s3_key"] = "cubic/ods_qlik/EDW.TEST__ct/20220101-112233444.csv.gz"

    assert job_helpers.from_catalog_kwargs(load, env) == {
        "database": "glue_db",
        "table_name": "cubic_ods_qlik__edw_test__ct",
        "additional_options": {"paths": [f's3://incoming/{load["s3_key"]}']},
        "transformation_ctx": "table_df_read",
    }


def test_df_with_updated_schema(spark_session: SparkSessionType) -> None:
    """
    Test creating a DataFrame with an updated schema and that
    the data is valid.

    Parameters
    ----------
    spark_session : list
        Fixture that contains the Spark Session to use
    """
    original_data = [
        (
            "test",
            "123",
            "null_123",
            "123.45",
            "null_123.45",
            "2022-01-01",
            "null_2022-01-01",
            "2022-01-01 01:23:45",
            "null_2022-01-01 01:23:45",
        )
    ]
    original_df = spark_session.createDataFrame(
        original_data,
        [
            "string_col",
            "bigint_col",
            "bigint_null_col",
            "double_col",
            "double_null_col",
            "date_col",
            "date_null_col",
            "timestamp_col",
            "timestamp_null_col",
        ],
    )

    updated_df = job_helpers.df_with_updated_schema(
        original_df,
        [
            {"name": "string_col", "type": "string"},
            {"name": "bigint_col", "type": "bigint"},
            {"name": "bigint_null_col", "type": "bigint"},
            {"name": "double_col", "type": "double"},
            {"name": "double_null_col", "type": "double"},
            {"name": "date_col", "type": "date"},
            {"name": "date_null_col", "type": "date"},
            {"name": "timestamp_col", "type": "timestamp"},
            {"name": "timestamp_null_col", "type": "timestamp"},
        ],
    )
    actual_schema = updated_df.schema.jsonValue()

    expected_schema = {
        "type": "struct",
        "fields": [
            {"name": "string_col", "type": "string", "nullable": True, "metadata": {}},
            {"name": "bigint_col", "type": "long", "nullable": True, "metadata": {}},
            {"name": "bigint_null_col", "type": "long", "nullable": True, "metadata": {}},
            {"name": "double_col", "type": "double", "nullable": True, "metadata": {}},
            {"name": "double_null_col", "type": "double", "nullable": True, "metadata": {}},
            {"name": "date_col", "type": "date", "nullable": True, "metadata": {}},
            {"name": "date_null_col", "type": "date", "nullable": True, "metadata": {}},
            {"name": "timestamp_col", "type": "timestamp", "nullable": True, "metadata": {}},
            {"name": "timestamp_null_col", "type": "timestamp", "nullable": True, "metadata": {}},
        ],
    }

    assert actual_schema == expected_schema

    # assert that the data is correctly casted
    assert updated_df.first() == Row(
        string_col="test",
        bigint_col=123,
        bigint_null_col=None,
        double_col=123.45,
        double_null_col=None,
        date_col=datetime.date(2022, 1, 1),
        date_null_col=None,
        timestamp_col=datetime.datetime(2022, 1, 1, 1, 23, 45),
        timestamp_null_col=None,
    )


def test_df_with_partition_columns(spark_session: SparkSessionType) -> None:
    """
    Test creating a DataFrame with the additional 'identifier' columns

    Parameters
    ----------
    spark_session : list
        Fixture that contains the Spark Session to use
    """

    # initial dataframe
    source_df = df_with_partition_columns(
        spark_session,
        [("test_1", "2022-01-01"), ("test_2", "2022-01-02")],
        ["name", "date"],
        [{"name": "identifier", "value": "identifier_1"}],
    )

    expected_data = [
        ("test_1", "2022-01-01", "identifier_1"),
        ("test_2", "2022-01-02", "identifier_1"),
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["name", "date", "identifier"])

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
        [
            {"name": "snapshot", "value": "snapshot_1"},
            {"name": "identifier", "value": "identifier_1"},
        ],
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
        [
            {"name": "snapshot", "value": "snapshot_2"},
            {"name": "identifier", "value": "identifier_2"},
        ],
    )

    # append to expected data, as partitions are different
    expected_data += [
        ("test_3", "2022-01-03", "snapshot_2", "identifier_2"),
        ("test_4", "2022-01-04", "snapshot_2", "identifier_2"),
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["name", "date", "snapshot", "identifier"])

    assert_equal_collections(parquet_df.collect(), expected_df.collect())

    # overwrite the 'identifier_1' partitition
    parquet_df = write_parquet(
        spark_session,
        parquet_path,
        [("test_5", "2022-01-05"), ("test_6", "2022-01-06")],
        ["name", "date"],
        [
            {"name": "snapshot", "value": "snapshot_1"},
            {"name": "identifier", "value": "identifier_1"},
        ],
    )

    expected_data = [
        ("test_3", "2022-01-03", "snapshot_2", "identifier_2"),
        ("test_4", "2022-01-04", "snapshot_2", "identifier_2"),
        ("test_5", "2022-01-05", "snapshot_1", "identifier_1"),
        ("test_6", "2022-01-06", "snapshot_1", "identifier_1"),
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["name", "date", "snapshot", "identifier"])

    assert_equal_collections(parquet_df.collect(), expected_df.collect())


def test_write_parquet_without_partition_columns(spark_session: SparkSessionType, tmp_path: str) -> None:
    """
    Test writing Parquet data without any partitions

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
        [],
    )

    expected_data = [
        ("test_1", "2022-01-01"),
        ("test_2", "2022-01-02"),
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["name", "date"])

    assert_equal_collections(parquet_df.collect(), expected_df.collect())
