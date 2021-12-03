
import os
import sys
import boto3
from dotenv import load_dotenv
from botocore.errorfactory import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import db
from db.models import cubic_qlik_table


# get enviroment variables from .env file
load_dotenv()

# get spark/glue context
sparkContext = SparkContext.getOrCreate()
glueContext = GlueContext(sparkContext)

# instatiate models
CubicQlikTable = cubic_qlik_table.CubicQlikTable

# instatiate clients
s3 = boto3.client('s3')
glue = boto3.client('glue')

# if we are on local, then make some updates to the clients
localCredentials = None
if bool(os.environ.get('LOCAL', 'False')):
  # if we have specified a boto profile to use, then override clients to use the specific session
  if os.environ.get('JOB_BOTO_PROFILE'):
    botoSession = boto3.Session(profile_name=os.environ.get('JOB_BOTO_PROFILE'))
    s3 = botoSession.client('s3')
    glue = botoSession.client('glue')
    # get current credentials
    credentials = botoSession.get_credentials()
    localCredentials = credentials.get_frozen_credentials()

# extract args passed into the AWS Glue job
args = getResolvedOptions(
  sys.argv,
  [
    'JOB_NAME',
    'object_key'
  ]
)

# get object's key that triggered the workflow/job
objectKey = args.get('object_key')

# head check to make sure the object is there
try:
  s3.head_object(Bucket=os.environ.get('S3_BUCKET_CUBIC_QLIK_LANDING'), Key=objectKey)
except ClientError:
  raise Exception() # @todo cover the case of a missing object

# get table records
tableRecs = []
with db.session() as session:
  tableRecs = session.query(CubicQlikTable).filter_by(deleted is None).all()

# determine which table the load is for
objectTable = None
loadType = ''
for tableRec in tableRecs:
  # batch
  if objectKey.startswith('{}{}/'.format(os.environ.get('S3_PREFIX', ''), tableRec.s3_prefix)):
    objectTable = tableRec
    loadType = 'batch'
  # cdc
  if objectKey.startswith('{}{}__ct/'.format(os.environ.get('S3_PREFIX', ''), tableRec.s3_prefix)):
    objectTable = tableRec
    loadType = 'cdc'

# get object and set up dataframe
# if localCredentials:
#   glueContext._jsc.hadoopConfiguration().set('fs.s3a.access.key', localCredentials.access_key)
#   glueContext._jsc.hadoopConfiguration().set('fs.s3a.secret.key', localCredentials.secret_key)
#   glueContext._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.amazonaws.com')
# loadDf = glueContext.create_dynamic_frame_from_options(
#   's3',
#   connection_options={
#     'paths': [ 's3://{}{}'.format(os.environ.get('S3_BUCKET_CUBIC_QLIK_LANDING'), objectKey) ]
#   }
# )
if localCredentials:
  sparkContext.hadoopConfiguration.set('fs.s3a.access.key', localCredentials.access_key)
  sparkContext.hadoopConfiguration.set('fs.s3a.secret.key', localCredentials.secret_key)
  sparkContext.hadoopConfiguration.set('fs.s3a.endpoint', 's3.amazonaws.com')
loadDf = sparkContext.option('inferSchema', 'true').read.csv(
  's3a://{}{}'.format(
    os.environ.get('S3_BUCKET_CUBIC_QLIK_LANDING'),
    objectKey
  ),
  header=True # first row is the header
)

# diverge here based on whether we are dealing with a batch load or a cdc load
if loadType == 'batch':
  # get schema from 'schema registry'
  schemaJson = ''
  try:
    schemaObject = s3.get_object(
      Bucket=os.environ.get('S3_BUCKET_DATA_PLATFORM'),
      Key='{}schema_registry/{}/batch_schema.json'.format(os.environ.get('S3_PREFIX', ''), tableRec.name)
    )
    # get the contents of object
    schemaJson = schemaObject.get('Body').read()
  except ClientError:
    raise Exception() # @todo cover the case of a missing schema

  # validate schema is the same as what we have agreed to, if not throw error
  if loadDf.schema.json() != schemaJson: # @todo there should be a better way to validate schema
    raise Exception() # @todo



if loadType == 'cdc':
  # get schema from 'schema registry'
  schemaJson = ''
  try:
    schemaObject = s3.get_object(
      Bucket=os.environ.get('S3_BUCKET_DATA_PLATFORM'),
      Key='{}schema_registry/{}/cdc_schema.json'.format(os.environ.get('S3_PREFIX', ''), tableRec.name)
    )
    # get the contents of object
    schemaJson = schemaObject.get('Body').read()
  except ClientError:
    raise Exception() # @todo cover the case of a missing schema

