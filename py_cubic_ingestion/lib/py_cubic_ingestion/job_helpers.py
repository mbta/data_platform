"""
Helper functions for `ingest_incoming` module. Also, allows for testing of some of the components
in the Glue Job.
"""

from mypy_boto3_glue.client import GlueClient
from py_cubic_ingestion import custom_udfs
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import DateType, DoubleType, LongType, TimestampType
from typing import Tuple
import json
import logging
import os


# helper variables
athena_type_to_spark_type = {
    "string": "string",
    "bigint": "long",
    "double": "double",
    "date": "date",
    "timestamp": "timestamp",
}

as_long_udf = udf(custom_udfs.as_long, LongType())
as_double_udf = udf(custom_udfs.as_double, DoubleType())
as_date_udf = udf(custom_udfs.as_date, DateType())
as_timestamp_udf = udf(custom_udfs.as_timestamp, TimestampType())


def parse_args(env_arg: str, input_arg: str) -> Tuple[dict, dict]:
    """
    Parses arguments for this Glue Job, and returns a dictionaries.

    Parameters
    ----------
    env_arg : str
        Environment variables as JSON-formatted string.
    input_arg : str
        Job data as JSON-formatted string.

    Returns
    -------
    dict, dict
        Dictionaries containing environment variables and job data.


    Examples
    --------
    >>> parse_job_arguments(
    ...   '{"GLUE_DATABASE_INCOMING": "db","S3_BUCKET_INCOMING": "incoming","S3_BUCKET_SPRINGBOARD": "springboard"}',
    ...   '{"loads": [{"s3_key": "...", "table_name": "...", "partition_columns": [...]}, ...]}'
    ... )
    ({'GLUE_DATABASE_INCOMING': 'db', 'S3_BUCKET_INCOMING': 'incoming', 'S3_BUCKET_SPRINGBOARD': 'springboard'},
    ...{'loads': [{'s3_key': '...', 'table_name': '...', 'partition_columns': [...]}, ...]})
    """

    log_prefix = "[py_cubic_ingestion] [job_helpers]"

    env_dict = {}
    try:
        env_dict = json.loads(env_arg)
    except json.JSONDecodeError as error:
        logging.error("%s Unable to decode `env_arg` JSON blob: %s", log_prefix, env_arg)
        raise error

    input_dict = {}
    try:
        input_dict = json.loads(input_arg)
    except json.JSONDecodeError as error:
        logging.error("%s Unable to decode `input_arg` JSON blob: %s", log_prefix, input_arg)
        raise error

    return (env_dict, input_dict)


def get_glue_table_schema_fields_by_load(glue_client: GlueClient, database_name: str, table_name: str) -> list:
    """
    Using the database and table name, fetch the table information so we can
    extract the schema fields. Field types are also converted from Athena
    types to Spark types in the process.

    Parameters
    ----------
    glue_client : GlueClient
        Boto3 client for getting the Glue table
    database_name : str
        Glue database to get the table from
    table_name : str
        Glue data catalog table name

    Returns
    -------
    list
        List of fields with name and type.
    """

    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)

    return [
        {"name": column["Name"], "type": athena_type_to_spark_type.get(column["Type"], "string")}
        for column in response["Table"]["StorageDescriptor"]["Columns"]
    ]


def get_glue_info(load: dict, env: dict) -> dict:
    """
    Determine source and destination information for glue based on load criterias

    Parameters
    ----------
    load : dict
        Load information
    env : dict
        Environment variables

    Returns
    -------
    dict
        Dictionary that contains information for the glue job
    """

    load_table_name = load["table_name"]
    load_s3_key = load["s3_key"]

    source_table_name = load_table_name
    destination_table_name = load_table_name
    source_key = load_s3_key
    destination_path = os.path.dirname(load_s3_key)

    # for change tracking loads, add a suffix to the table names
    if destination_path.endswith("__ct"):
        source_table_name = f"{source_table_name}__ct"
        destination_table_name = f"{destination_table_name}__ct"

    # for raw loads, add a prefix to the table_name and adjust the destination path
    if load["is_raw"]:
        destination_table_name = f"raw_{destination_table_name}"
        destination_path = f"raw/{destination_path}"

    return {
        "source_table_name": source_table_name,
        "destination_table_name": destination_table_name,
        "source_key": f's3://{env["S3_BUCKET_INCOMING"]}/{env.get("S3_BUCKET_PREFIX_INCOMING", "")}{source_key}',
        "destination_path": f's3a://{env["S3_BUCKET_SPRINGBOARD"]}/'
        f'{env.get("S3_BUCKET_PREFIX_SPRINGBOARD", "")}{destination_path}/',
    }


def df_with_updated_schema(df: DataFrame, schema_fields: list) -> DataFrame:
    """
    Construct a new DataFrame with an updated schema. Columns will
    be cast with the indicated type. If unable to cast, Spark will
    set the field to NULL.

    Parameters
    ----------
    df : DataFrame
        DataFrame containing the data
    schema_fields : list
        List of fields that will be used to update the schema

    Returns
    -------
    DataFrame
        Updated DataFrame containing an updated schema
    """

    columns = []
    for field in schema_fields:
        field_name = field["name"]
        column = col(field_name)

        # override if we can cast successfully
        if field["type"] == "long":
            column = as_long_udf(field_name)
        elif field["type"] == "double":
            column = as_double_udf(field_name)
        elif field["type"] == "date":
            column = as_date_udf(field_name)
        elif field["type"] == "timestamp":
            column = as_timestamp_udf(field_name)

        columns.append(column.alias(field_name))

    return df.select(columns)


def df_with_partition_columns(df: DataFrame, partition_columns: list) -> DataFrame:
    """
    Construct a new DataFrame with partition columns added

    Parameters
    ----------
    df : DataFrame
        DataFrame containing the data
    partition_columns : list
        List of dicts with partition information

    Returns
    -------
    DataFrame
        Updated DataFrame containing the partition columns
    """

    for column in partition_columns:
        df = df.withColumn(column["name"], lit(column["value"]))

    return df


def write_parquet(df: DataFrame, partition_columns: list, destination: str) -> None:
    """
    Write a DataFrame to Parquet in the designated path

    Parameters
    ----------
    df : DataFrame
        DataFrame containing the data
    partition_columns : list
        List of dicts with partition information
    destination : str
        Path to write to
    """

    df_with_partition_columns(df, partition_columns).write.mode("overwrite").partitionBy(
        [column["name"] for column in partition_columns]
    ).parquet(destination)
