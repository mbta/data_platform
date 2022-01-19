
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


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV', 'INPUT'])

# read arguments
jobName = args.get('JOB_NAME', 'cubic_ods_ingest')
envDict = json.loads(args.get('ENV', '{}'))
inputDict = json.loads(args.get('INPUT', '{}'))

# create job using the glue context
job = Job(glueContext)
# initialize job
job.init(jobName, args)

# run glue transformations for each cubic ods table
for table in inputDict.get('tables', []):
  dataCatalogTableName = 'incoming__{}'.format(table.get('name'))
  if table.get('load_is_cdc', False):
    dataCatalogTableName += '__ct'

  # create table dataframe using the data catalog table in glue
  tableDF = glueContext.create_dynamic_frame.from_catalog(
    database=envDict.get('GLUE_DATABASE_NAME'),
    table_name=dataCatalogTableName,
    additional_options={
      'paths': table.get('load_s3_keys')
    },
    transformation_ctx='{}_table_df_read'.format(jobName)
  )
  # convert to spark dataframe so we can use withColumn
  tableSparkDF = tableDF.toDF()

  # determine partitions
  partitionKeys = [ 'snapshot' ]
  # add a new column for 'snapshot' and set to the value of the table's snapshot value
  tableSparkDF = tableSparkDF.withColumn('snapshot', lit(table.get('snapshot')))
  # if cdc load, then we want to add another partition for the file and one other column to the df
  if table.get('load_is_cdc', False):
    partitionKeys.append('file')

    # determine the file name. for cdc, there is always one load file
    fileName = os.path.basename(table.get('load_s3_keys')[0])
    tableSparkDF = tableSparkDF.withColumn('file', lit(fileName))

  # write out to parquet
  glueContext.write_dynamic_frame.from_options(
    frame=tableDF,
    connection_type='s3',
    format='glueparquet',
    connection_options={
      'path': 's3://{}/{}{}.parquet/'.format(
        envDict.get('S3_BUCKET_SPRINGBOARD'),
        envDict.get('S3_BUCKET_SPRINGBOARD_PREFIX'), # used in local setups
        table.get('s3_prefix')
      ),
      'partitionKeys': partitionKeys
    },
    format_options={ 'compression': 'gzip' },
    transformation_ctx='{}_table_df_write_parquet'.format(jobName)
  )

job.commit()
