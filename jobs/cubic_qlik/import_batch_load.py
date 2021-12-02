
import os
import sys
import boto3
from botocore.errorfactory import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


# instatiate clients
s3 = boto3.client('s3')

# if we are on local, then make some updates to the clients
if os.environ.get('ENV') == 'local':
  # if we have specified a boto profile to use, then override clients to use the specific session
  if os.environ.get('JOB_BOTO_PROFILE'):
    botoSession = boto3.Session(profile_name=os.environ.get('JOB_BOTO_PROFILE'))
    s3 = botoSession.client('s3')

# get glue context
glueContext = GlueContext(SparkContext.getOrCreate())

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
  s3.head_object(Bucket=os.environ.get('S3_BUCKET_CUBIC_QLIK_LANDING'), Key=)
except ClientError:
  # Not found
  pass

# extract table prefix from object key

# get table record

# insert record into DB to indicate that job is processing object


