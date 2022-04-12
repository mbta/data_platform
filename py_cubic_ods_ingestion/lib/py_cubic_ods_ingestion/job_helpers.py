"""
Helper functions for `ingest_incoming` module. Also, allows for testing of some of the components
in the Glue Job.
"""

import logging
import json
import typing


def removeprefix(text: str, prefix: str) -> str:
    """
    A (not-exact) shim for `str.removeprefix()` in Python 3.9+
    """
    if text.startswith(prefix):
        return text[len(prefix) :]

    return text


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

    log_prefix = "[py_cubic_ods_ingestion][job_helpers]"

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


def get_table_name_suffix(load_table_s3_prefix: str) -> str:
    """
    Table name suffix should be '__ct' if the S3 prefix is from change tracking.
    """

    return "__ct" if load_table_s3_prefix.endswith("__ct/") else ""
