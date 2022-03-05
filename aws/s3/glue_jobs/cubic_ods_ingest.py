
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


glueContext = GlueContext(SparkContext())
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV', 'INPUT'])

# read arguments
jobName = args.get('JOB_NAME', 'cubic_ods_ingest')
envDict = json.loads(args.get('ENV', '{}'))
inputDict = json.loads(args.get('INPUT', '{}'))

# create job using the glue context
job = Job(glueContext)
# initialize job
job.init(jobName, args)

# run glue transformations for each cubic ods load
for load in inputDict.get('loads', []):
  # determine the glue catalog table name
  dataCatalogTableName = 'incoming__{}'.format(load.get('table_name'))
  # and prefix to use when saving
  loadTableS3Prefix = load.get('table_s3_prefix')

  # for cdc, we want to suffix by '__ct'
  if os.path.dirname(load.get('s3_key')).endswith('__ct'):
    dataCatalogTableName += '__ct'
    loadTableS3Prefix = loadTableS3Prefix[:-1] + '__ct/'

  # create table dataframe using the data catalog table in glue
  tableDF = glueContext.create_dynamic_frame.from_catalog(
    database=envDict.get('GLUE_DATABASE_NAME'),
    table_name=dataCatalogTableName,
    additional_options={
      'paths': ['s3://{}/{}'.format(envDict.get('S3_BUCKET_INCOMING'), load.get('s3_key'))]
    },
    transformation_ctx='{}_table_df_read'.format(jobName)
  )

  # convert to spark dataframe so we can use the withColumn functionality
  tableSparkDF = tableDF.toDF()
  # add partition columns
  tableSparkDF = tableSparkDF.\
                    withColumn('snapshot', lit(load.get('snapshot'))).\
                    withColumn('identifier', lit(os.path.basename(load.get('s3_key'))))
  # write out to springboard bucket with overwrite mode
  tableSparkDF.write.\
    mode('overwrite').\
    partitionBy('snapshot', 'identifier').\
    parquet('s3a://{}/{}{}'.format(
      envDict.get('S3_BUCKET_SPRINGBOARD'),
      envDict.get('S3_BUCKET_PREFIX_SPRINGBOARD'), # used in local setups
      loadTableS3Prefix
    ))

job.commit()
