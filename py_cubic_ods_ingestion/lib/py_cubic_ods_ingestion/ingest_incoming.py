"""
Module contains the 'run' function utilized by the Glue Job defined in `aws/s3/glue_jobs/cubic_ods_ingest.py`.
"""

import os
import sys
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from pyspark.context import SparkContext  # pylint: disable=import-error
from pyspark.sql.functions import lit  # pylint: disable=import-error

from py_cubic_ods_ingestion import job_helpers


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

    # run glue transformations for each cubic ods load
    for load in input_dict.get("loads", []):
        load_s3_key = load["s3_key"]
        load_table_s3_prefix = job_helpers.removeprefix(
            f"{os.path.dirname(load_s3_key)}/",
            env_dict.get("S3_BUCKET_PREFIX_INCOMING", ""),
        )
        data_catalog_table_name = load["table_name"]

        # for cdc, we want to suffix table name by '__ct'
        if load_table_s3_prefix.endswith("__ct"):
            data_catalog_table_name += "__ct"

        # create table dataframe using the data catalog table in glue
        table_df = glue_context.create_dynamic_frame.from_catalog(
            database=env_dict["GLUE_DATABASE_INCOMING"],
            table_name=data_catalog_table_name,
            additional_options={"paths": [f's3://{env_dict["S3_BUCKET_INCOMING"]}/{load_s3_key}']},
            transformation_ctx=f"{job_name}_table_df_read",
        )

        # convert to spark dataframe so we can use the withColumn functionality
        table_spark_df = table_df.toDF()
        # add partition columns
        table_spark_df = table_spark_df.withColumn("snapshot", lit(load["snapshot"])).withColumn(
            "identifier", lit(os.path.basename(load_s3_key))
        )

        # write out to springboard bucket with overwrite mode
        table_spark_df.write.mode("overwrite").partitionBy("snapshot", "identifier").parquet(
            f's3a://{env_dict["S3_BUCKET_SPRINGBOARD"]}/'
            f'{env_dict.get("S3_BUCKET_PREFIX_SPRINGBOARD", "")}{load_table_s3_prefix}'
        )

    job.commit()
