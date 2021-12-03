
import json
import datetime
from dateutil.tz import tzlocal
import os
import argparse
import boto3
from dotenv import load_dotenv

import db
from db.models import cubic_qlik_table, cubic_qlik_batch_load, cubic_qlik_cdc_load


# get enviroment variables from .env file
load_dotenv()

# initialize clients (this uses the default session, i.e. instance profile/role)
s3 = boto3.client('s3')
glue = boto3.client('glue')

# if we are on local, then make some updates to the clients
if bool(os.environ.get('LOCAL', 'False')):
  # if we have specified a boto profile to use, then override clients to use the specific session
  if os.environ.get('BATCH_BOTO_PROFILE'):
    botoSession = boto3.Session(profile_name=os.environ.get('BATCH_BOTO_PROFILE'))
    s3 = botoSession.client('s3')
    glue = botoSession.client('glue')



def main():
  response = glue.get_table(DatabaseName='gg-test', Name='edw.sample')
  print(json.dumps({'Table': {'Name': 'edw.sample', 'DatabaseName': 'gg-test', 'Retention': 0, 'StorageDescriptor': {'Columns': [{'Name': 'sample_id', 'Type': 'bigint'}, {'Name': 'sample_name', 'Type': 'string'}, {'Name': 'edw_inserted_dtm', 'Type': 'timestamp'}, {'Name': 'edw_updated_dtm', 'Type': 'timestamp'}], 'Location': 's3://mbta-ctd-datalake-ps-tmp/cubic_qlik_ingest/gg-test/cubic_qlik/EDW.SAMPLE/', 'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat', 'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', 'Compressed': False, 'NumberOfBuckets': 0, 'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde', 'Parameters': {'separatorChar': ','}}, 'SortColumns': [], 'StoredAsSubDirectories': False}, 'PartitionKeys': [], 'TableType': 'EXTERNAL_TABLE', 'Parameters': {'classification': 'csv'}, 'CreatedBy': 'arn:aws:iam::434035161053:user/ggjura', 'IsRegisteredWithLakeFormation': False, 'CatalogId': '434035161053'}, 'ResponseMetadata': {'RequestId': 'd31cd286-b288-4c97-ad57-60a6130eb95b', 'HTTPStatusCode': 200, 'HTTPHeaders': {'date': 'Thu, 02 Dec 2021 15:27:41 GMT', 'content-type': 'application/x-amz-json-1.1', 'content-length': '1005', 'connection': 'keep-alive', 'x-amzn-requestid': 'd31cd286-b288-4c97-ad57-60a6130eb95b'}, 'RetryAttempts': 0}}))



# script controller
if __name__ == '__main__':
  main()
