
import sys
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

# get object's key that triggered the job
key = args.get('key')
