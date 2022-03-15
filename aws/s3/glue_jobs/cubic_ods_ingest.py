import os
import logging
import json
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *


def removeprefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix) :]

    return text


# @todo use function annotations
def parse_job_arguments(env_arg, input_arg):
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

    env_dict = json.loads(env_arg)
    input_dict = json.loads(input_arg)

    return env_dict, input_dict


def run():
    """
    Reads CSV files from Incoming bucket, and writes them as Parquet files in the
    Springboard bucket.
    """

    glue_context = GlueContext(SparkContext())
    spark = glue_context.spark_session
    # spark config for allowing overwriting a specific partition
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV", "INPUT"])

    # read arguments
    job_name = args["JOB_NAME"]
    env_dict, input_dict = parse_job_arguments(args["ENV"], args["INPUT"])

    # create job using the glue context
    job = Job(glue_context)
    # initialize job
    job.init(job_name, args)

    # run glue transformations for each cubic ods load
    for load in input_dict.get("loads", []):
        load_s3_key = load["s3_key"]
        load_table_s3_prefix = removeprefix(
            f"{os.path.dirname(load_s3_key)}/",
            env_dict.get("S3_BUCKET_PREFIX_INCOMING", ""),
        )
        data_catalog_table_name = f'incoming__{load["table_name"]}'

        # for cdc, we want to suffix table name by '__ct'
        if load_table_s3_prefix.endswith("__ct"):
            data_catalog_table_name += "__ct"

        # create table dataframe using the data catalog table in glue
        table_df = glue_context.create_dynamic_frame.from_catalog(
            database=env_dict["GLUE_DATABASE_NAME"],
            table_name=data_catalog_table_name,
            additional_options={
                "paths": [f's3://{env_dict["S3_BUCKET_INCOMING"]}/{load_s3_key}']
            },
            transformation_ctx=f"{job_name}_table_df_read",
        )

        # convert to spark dataframe so we can use the withColumn functionality
        table_spark_df = table_df.toDF()
        # add partition columns
        table_spark_df = table_spark_df.withColumn(
            "snapshot", lit(load["snapshot"])
        ).withColumn("identifier", lit(os.path.basename(load_s3_key)))

        # write out to springboard bucket with overwrite mode
        table_spark_df.write.mode("overwrite").partitionBy(
            "snapshot", "identifier"
        ).parquet(
            f's3a://{env_dict["S3_BUCKET_SPRINGBOARD"]}/'
            f'{env_dict.get("S3_BUCKET_PREFIX_SPRINGBOARD", "")}{load_table_s3_prefix}'
        )

    job.commit()

if __name__ == "__main__":
    run()
