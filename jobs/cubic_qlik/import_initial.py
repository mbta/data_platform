
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


# get glue context
glueContext = GlueContext(SparkContext.getOrCreate())

# extract args passed into the AWS Glue job
args = getResolvedOptions(
  sys.argv,
  [
    'JOB_NAME',
    'key'
  ]
)

# get object's key that triggered the workflow/job
key = args.get('key')

# head check to make sure the object is there

# insert record into DB to indicate



