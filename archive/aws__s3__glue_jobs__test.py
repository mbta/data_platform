
import logging
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())

# extract args passed into the AWS Glue job
args = getResolvedOptions(
  sys.argv,
  [
    'JOB_NAME'
  ]
)
jobName = args.get('JOB_NAME')
tables = args.get('TABLES', '{}')

filePaths = [
  "s3://mbta-ctd-datalake-ps-tmp/gg/incoming/EDW.SAMPLE/LOAD1.csv",
  "s3://mbta-ctd-datalake-ps-tmp/gg/incoming/EDW.SAMPLE/LOAD2.csv"
]

# data_source = glueContext.getSource("file", paths=filePaths)
# data_source.setFormat("csv", options={
#   'withHeader': True,
#   'skipFirst': True,
#   'separator': ',',
#   'quoteChar': '"'
# })
# myFrame = data_source.getFrame()
# logging.error(myFrame.toDF().schema.json())


# df = glueContext.create_dynamic_frame.from_options(
#   connection_type="s3",
#   connection_options={
#     "paths": filePaths
#   },
#   format="csv",
#   format_options={
#     'withHeader': True,
#     'skipFirst': True,
#     'separator': ',',
#     'quoteChar': '"'
#   }
# )
# logging.error(df.toDF().schema.json())

df = glueContext.create_dynamic_frame.from_catalog(
  database="gg-test",
  table_name="test",
  additional_options={
    "paths": filePaths,
    "isFailFast": "true"
  }
)
logging.error(df.toDF().schema.json())



# def init():
#   global botoCredentials
#   global s3Client

#   # instatiate clients
#   s3Client = boto3.client('s3')

#   # if we are on local, then make some updates to the clients
#   if bool(os.environ.get('LOCAL', 'False')):
#     # if we have specified a boto profile to use, then override clients to use the specific session
#     if os.environ.get('JOB_BOTO_PROFILE'):
#       botoSession = boto3.Session(profile_name=os.environ.get('JOB_BOTO_PROFILE'))
#       s3Client = botoSession.client('s3')
#       # get current credentials
#       credentials = botoSession.get_credentials()
#       botoCredentials = credentials.get_frozen_credentials()

# def job(jobName, objectKey):
#   # instatiate models
#   CubicQlikTable = cubic_qlik_table.CubicQlikTable
#   CubicQlikBatchLoad = cubic_qlik_batch_load.CubicQlikBatchLoad
#   CubicQlikCDCLoad = cubic_qlik_cdc_load.CubicQlikCDCLoad

#   # create Spark session
#   spark = SparkSession.builder.getOrCreate()
#   # if we are running on local, credentials will need to be set explicitly and new session created
#   if botoCredentials:
#     spark = (
#       SparkSession.builder
#         .config('fs.s3a.access.key', botoCredentials.access_key)
#         .config('fs.s3a.secret.key', botoCredentials.secret_key)
#         .getOrCreate()
#     )

#   # get context
#   # sparkContext = SparkContext.getOrCreate()
#   # glueContext = GlueContext(sparkContext)

#   # set some object key variables
#   objectExtension = '.csv' # @todo change to '.csv.gz'
#   objectFilename = os.path.basename(objectKey)
#   # double check we have the right extension
#   if not objectFilename.endswith(objectExtension):
#     raise Exception() # @todo wrong extension and possibly wrong file type

#   # head check to make sure the object is there
#   s3Client.head_object(
#     Bucket=os.environ.get('S3_BUCKET_CUBIC_QLIK_LANDING'),
#     Key=objectKey
#   )

#   # get table records
#   tableRecs = []
#   with dbSession() as db:
#     tableRecs = db.query(CubicQlikTable).all() # @todo filter deleted

#   # determine which table the load is for
#   objectTableRec = None
#   loadType = ''
#   for tableRec in tableRecs:
#     # batch
#     if objectKey.startswith('{}{}/'.format(os.environ.get('S3_PREFIX_CUBIC_QLIK_LANDING', ''), tableRec.name)):
#       objectTableRec = tableRec
#       loadType = 'batch'
#     # cdc
#     if objectKey.startswith('{}{}__ct/'.format(os.environ.get('S3_PREFIX_CUBIC_QLIK_LANDING', ''), tableRec.name)):
#       objectTableRec = tableRec
#       loadType = 'cdc'

#   # raise error if the object's table rec is not found
#   if not objectTableRec:
#     raise Exception() # @todo maybe this is just a log and ignore

#   # diverge here based on whether we are dealing with a batch load or a cdc load
#   if loadType == 'batch':

#     # instantiate a session to make sure we work with the database
#     with dbSession() as db:
#       # see if we have a record already for this load
#       batchLoadRec = db.query(CubicQlikBatchLoad).filter(CubicQlikBatchLoad.s3_key==objectKey).first()
#       # if we don't have a record, create one so we can keep track of loads
#       if not batchLoadRec:
#         batchLoadRec = CubicQlikBatchLoad(**{
#           'table_id': objectTableRec.id,
#           's3_key': objectKey,
#           'status': 'ingesting'
#         })
#         db.add(batchLoadRec)
#         db.commit()
#       # otherwise update the status to ingesting
#       else:
#         batchLoadRec.status = 'ingesting'
#         db.commit()

#       # get object and set up dataframe
#       # if botoCredentials:
#       #   glueContext._jsc.hadoopConfiguration().set('fs.s3a.access.key', botoCredentials.access_key)
#       #   glueContext._jsc.hadoopConfiguration().set('fs.s3a.secret.key', botoCredentials.secret_key)
#       #   glueContext._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.amazonaws.com')
#       # loadDf = glueContext.create_dynamic_frame_from_options(
#       #   's3',
#       #   connection_options={
#       #     'paths': [ 's3://{}{}'.format(os.environ.get('S3_BUCKET_CUBIC_QLIK_LANDING'), objectKey) ]
#       #   }
#       # )
#       loadDf = spark.read.option('inferSchema', 'true').csv(
#         's3a://{}/{}'.format(os.environ.get('S3_BUCKET_CUBIC_QLIK_LANDING'), objectKey),
#         header=True # first row is the header
#       )

#       # get schema from 'schema registry'
#       schemaObject = s3Client.get_object(
#         Bucket=os.environ.get('S3_BUCKET_DATA_PLATFORM'),
#         Key='{}schema_registry/{}/schema.json'.format(os.environ.get('S3_PREFIX_DATA_PLATFORM', ''), objectTableRec.name)
#       )
#       # get the contents of object
#       schemaJson = schemaObject.get('Body').read().decode('utf-8')

#       # validate schema is the same as what we have agreed to, if not throw error
#       if loadDf.schema.json() != schemaJson: # @todo there should be a better way to validate schema
#         raise Exception() # @todo

#       # copy object to archive bucket
#       s3Client.copy_object(
#         CopySource='{}/{}'.format(os.environ.get('S3_BUCKET_CUBIC_QLIK_LANDING'), objectKey),
#         Bucket=os.environ.get('S3_BUCKET_CUBIC_QLIK_ARCHIVE'),
#         Key='{}{}/{}'.format(os.environ.get('S3_PREFIX_CUBIC_QLIK_ARCHIVE'), objectTableRec.name, objectFilename)
#       )

#       # set the load records status to 'ingested'
#       batchLoadRec.status = 'ingested'
#       db.commit()

#       # delete object from landing bucket
#       s3Client.delete_object(
#         Bucket=os.environ.get('S3_BUCKET_CUBIC_QLIK_LANDING'),
#         Key=objectKey
#       )

#   # if loadType == 'cdc':
#   #   # get schema from 'schema registry'
#   #   schemaJson = ''
#   #   try:
#   #     schemaObject = s3Client.get_object(
#   #       Bucket=os.environ.get('S3_BUCKET_DATA_PLATFORM'),
#   #       Key='{}schema_registry/{}/cdc_schema.json'.format(os.environ.get('S3_PREFIX_DATA_PLATFORM', ''), objectTableRec.name)
#   #     )
#   #     # get the contents of object
#   #     schemaJson = schemaObject.get('Body').read()
#   #   except ClientError:
#   #     raise Exception() # @todo cover the case of a missing schema

# def error(jobName, objectKey):
#   pass

# def run():

#   # initialize globals
#   init()

#   # try:
#   # run job
#   job(jobName, objectKey)
#   # except Exception:
#   #   error(jobName, objectKey)

