"""
Module contains the 'run' function utilized by the Glue Job defined in `aws/s3/glue_jobs/cubic_ods_ingest.py`.
"""

import os
import sys
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from pyspark.context import SparkContext

from py_cubic_ingestion import job_helpers


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
        from_catalog_kwargs = job_helpers.from_catalog_kwargs(load, env_dict)
        # create table dataframe using the data catalog table in glue
        table_df = glue_context.create_dynamic_frame.from_catalog(**from_catalog_kwargs)

        # add partition columns
        table_spark_df = job_helpers.df_with_identifier(
            table_df.toDF(),  # convert to spark df so we can use the withColumn functionality
            os.path.basename(load["s3_key"]),
        )

        # write out to springboard bucket using the same prefix as incoming
        job_helpers.write_parquet(
            table_spark_df,
            f's3a://{env_dict["S3_BUCKET_SPRINGBOARD"]}'
            f'/{env_dict.get("S3_BUCKET_PREFIX_SPRINGBOARD", "")}{os.path.dirname(load["s3_key"])}/',
        )

    job.commit()
