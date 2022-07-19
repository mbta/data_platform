"""
Module contains the 'run' function utilized by the Glue Job defined
in `aws/s3/glue_jobs/cubic_ingestion/ingest_incoming.py`.
"""

from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from py_cubic_ingestion import job_helpers
from pyspark.context import SparkContext
import boto3
import sys


def run() -> None:
    """
    Reads CSV files from Incoming bucket, and writes them as Parquet files in the
    Springboard bucket.
    """

    glue_context = GlueContext(SparkContext())
    spark = glue_context.spark_session
    # spark config for allowing overwriting a specific partition
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV", "INPUT"])

    # glue client
    glue_client = boto3.client("glue")

    # read arguments
    job_name = args["JOB_NAME"]
    # parse out ENV and INPUT into dicts
    env_dict, input_dict = job_helpers.parse_args(args["ENV"], args["INPUT"])

    # create job using the glue context
    job = Job(glue_context)
    # initialize job
    job.init(job_name, args)

    # run glue transformations for each cubic load
    for load in input_dict.get("loads", []):
        glue_info = job_helpers.get_glue_info(load, env_dict)

        destination_schema_fields = job_helpers.get_glue_table_schema_fields_by_load(
            glue_client,
            env_dict["GLUE_DATABASE_SPRINGBOARD"],
            glue_info["destination_table_name"],
        )

        # create table dataframe using the data catalog table in glue
        table_df = glue_context.create_dynamic_frame.from_catalog(
            database=env_dict["GLUE_DATABASE_INCOMING"],
            table_name=glue_info["source_table_name"],
            additional_options={"paths": [glue_info["source_key"]]},
            transformation_ctx="table_df_read",
        )

        # cast columns with the springboard schema
        updated_table_df = job_helpers.df_with_updated_schema(table_df.toDF(), destination_schema_fields)

        # write out to springboard bucket using the same prefix as incoming
        job_helpers.write_parquet(updated_table_df, load.get("partition_columns", []), glue_info["destination_path"])

    job.commit()
