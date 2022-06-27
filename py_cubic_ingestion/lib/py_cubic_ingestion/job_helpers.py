"""
Helper functions for `ingest_incoming` module. Also, allows for testing of some of the components
in the Glue Job.
"""

from typing import Tuple
from py_cubic_ingestion import custom_udfs
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit
import boto3
import logging
import json
import os


# helper variables
glue_client = boto3.client("glue")

athena_type_to_spark_type = {
    "string": "string",
    "bigint": "long",
    "double": "double",
    "date": "date",
    "timestamp": "timestamp",
}


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


def table_name_suffix(load_table_s3_prefix: str) -> str:
    """
    Table name suffix should be '__ct' if the S3 prefix is from change tracking.

    Parameters
    ----------
    load_table_s3_prefix : str
        The load's prefix as determined by its S3 key.

    Returns
    -------
    str
        "__ct" or "" depending on the load's prefix
    """

    return "__ct" if load_table_s3_prefix.endswith("__ct/") else ""


def get_glue_table_schema_fields_by_load(glue_database_name: str, load: dict) -> list:
    """
    Using the load's information, fetch the table information so we can
    extract the schema fields. Field types are also converted from Athena
    types to Spark types in the process.

    Parameters
    ----------
    glue_database_name : str
        Glue database to get the table from
    load : dict
        Load information

    Returns
    -------
    list
        List of fields with name and type.
    """

    load_s3_key = load["s3_key"]

    # get suffix for data catalog
    data_catalog_table_name_suffix = table_name_suffix(
        f"{os.path.dirname(load_s3_key)}/",
    )

    response = glue_client.get_table(
        DatabaseName=glue_database_name, Name=f'{load["table_name"]}{(data_catalog_table_name_suffix)}'
    )

    return [
        {"name": column["Name"], "type": athena_type_to_spark_type.get(column["Type"], "string")}
        for column in response["Table"]["StorageDescriptor"]["Columns"]
    ]


def from_catalog_kwargs(load: dict, env: dict) -> dict:
    """
    Construct kwargs for 'from_catalog'

    Parameters
    ----------
    load : dict
        Load information
    env : dict
        Environment variables

    Returns
    -------
    dict
        Dictionary that will be passed in as kwargs
    """

    load_s3_key = load["s3_key"]

    # get suffix for data catalog
    data_catalog_table_name_suffix = table_name_suffix(
        f"{os.path.dirname(load_s3_key)}/",
    )

    return {
        "database": env["GLUE_DATABASE_INCOMING"],
        "table_name": f'{load["table_name"]}{(data_catalog_table_name_suffix)}',
        "additional_options": {
            "paths": [f's3://{env["S3_BUCKET_INCOMING"]}/{env.get("S3_BUCKET_PREFIX_INCOMING", "")}{load_s3_key}']
        },
        "transformation_ctx": "table_df_read",
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

    column_statements = []
    for field in schema_fields:
        if field["type"] == "long":
            column_statements.append(custom_udfs.as_long(field["name"]).alias(field["name"]))
        elif field["type"] == "double":
            column_statements.append(custom_udfs.as_double(field["name"]).alias(field["name"]))
        elif field["type"] == "date":
            column_statements.append(custom_udfs.as_date(field["name"]).alias(field["name"]))
        elif field["type"] == "timestamp":
            column_statements.append(custom_udfs.as_timestamp(field["name"]).alias(field["name"]))
        else:
            column_statements.append(field["name"])

        # column_statements.append(f'cast ({field["name"]} as {field["type"]}) as {field["name"]}')

    return df.select(column_statements)


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
