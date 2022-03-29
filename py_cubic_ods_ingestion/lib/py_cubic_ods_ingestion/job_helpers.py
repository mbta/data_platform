import typing
import json


def removeprefix(text: str, prefix: str) -> str:
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
    ...   '{"GLUE_DATABASE_NAME": "db","S3_BUCKET_INCOMING": "incoming","S3_BUCKET_SPRINGBOARD": "springboard"}',
    ...   '{"loads":[{"s3_key":"s3/prefix/key","snapshot":"20220101","table_name":"table"}]}'
    ... )
    ({'GLUE_DATABASE_NAME': 'db', 'S3_BUCKET_INCOMING': 'incoming', 'S3_BUCKET_SPRINGBOARD': 'springboard'}, {'loads': [{'s3_key': 's3/prefix/key', 'snapshot': '20220101', 'table_name': 'table'}]})
    """

    env_dict = {}
    try:
        env_dict = json.loads(env_arg)
    except json.JSONDecodeError as e:
        pass

    input_dict = {}
    try:
        input_dict = json.loads(input_arg)
    except json.JSONDecodeError as e:
        pass

    return (env_dict, input_dict)
