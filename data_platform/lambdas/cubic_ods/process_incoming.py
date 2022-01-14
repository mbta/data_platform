
import os
import boto3
import botocore
import logging

from data_platform.db import dbSession
from data_platform.db.models.cubic_ods_table import CubicODSTable
from data_platform.db.models.cubic_ods_load import CubicODSCubicODSLoad


# get enviroment variables from .env file, if not already set
load_dotenv()

def run(loadKey):

  # intialize boto clients
  s3 = boto3.client('s3')

  # get s3 info for load
  try:
    loadS3Info = s3.head_object(
      Bucket=os.environ.get('S3_BUCKET_INCOMING'),
      Key=loadKey
    )
  # if any exception, return with error
  except s3.exceptions.NoSuchKey as e:
    logging.error('[data_platform] [cubic_ods] [lambdas] [process_incoming]: {}'.format(e))
    return {
      'type': 'error',
      'message': e
    }

  # start a db session to use with updating database
  with dbSession() as db:
    # get all tables that are not deleted
    tableRecs = db.query(CubicODSTable).filter(CubicODSTable.deleted is not None).all()

    # determine which table the load is for
    tableAvailable = False # for keeping track of if we find the table
    for tableRec in tableRecs:
      if loadKey.startswith('{}{}/'.format(os.environ.get('S3_BUCKET_INCOMING_PREFIX'), tableRec.s3_prefix)):
        tableAvailable = True

        # if we are trying to insert a load that matches the snapshot key, we should update the snapshot
        # value for the table
        if loadKey == '{}{}'.format(os.environ.get('S3_BUCKET_INCOMING_PREFIX'), tableRec.snapshot_s3_key):
          tableRec.snapshot = loadS3Info.get('LastModified')

        # insert a load record
        loadRec = CubicODSLoad(**{
          'table_id': tableRec.id,
          'status': 'ready',
          'snapshot': tableRec.snapshot,
          's3_key': loadKey,
          's3_modified': loadS3Info.get('LastModified'),
        })
        db.add(loadRec)

        # commit load record insert, and any update to the table snapshot
        db.commit()

        # we found the table and inserted the record, so stop
        break

    # if we didn't find the table, let others know by logging an error
    if not tableAvailable:
      errorMessage = 'Cubic ODS table doesn\'t exist for the load object: {}'.format(loadKey)

      logging.error('[data_platform] [lambdas] [cubic_ods] [process_incoming]: {}'.format(errorMessage))
      return {
        'type': 'error',
        'message': errorMessage
      }

  return {
    'type': 'success'
  }

if __name__ == '__main__':
  run()
