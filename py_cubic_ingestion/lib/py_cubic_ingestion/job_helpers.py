"""
Helper functions for `ingest_incoming` module. Also, allows for testing of some of the components
in the Glue Job.
"""

import os
import logging
import json
import typing
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit


def parse_args(env_arg: str, input_arg: str) -> typing.Tuple[dict, dict]:
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
    ...   '{"loads":[{"s3_key":"s3/prefix/key","snapshot":"20220101","table_name":"table"}]}'
    ... )
    ({'GLUE_DATABASE_INCOMING': 'db', 'S3_BUCKET_INCOMING': 'incoming', 'S3_BUCKET_SPRINGBOARD': 'springboard'},
    ...{'loads': [{'s3_key': 's3/prefix/key', 'snapshot': '20220101', 'table_name': 'table'}]})
    """

    log_prefix = "[py_cubic_ingestion][job_helpers]"

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


def df_with_identifier(df: DataFrame, identifier_val: str) -> DataFrame:
    """
    Construct a new DataFrame with 'identifier' column added

    Parameters
    ----------
    df : DataFrame
        DataFrame containing the data
    identifier_val : str
        Value of the 'identifier' column

    Returns
    -------
    DataFrame
        Updated DataFrame containing the 'identifier' column
    """

    return df.withColumn("identifier", lit(identifier_val))


def write_parquet(df: DataFrame, destination: str) -> None:
    """
    Write a DataFrame to Parquet in the designated path

    Parameters
    ----------
    df : DataFrame
        DataFrame containing the data
    destination : str
        Path to write to
    """

    df.write.mode("overwrite").partitionBy("identifier").parquet(destination)
