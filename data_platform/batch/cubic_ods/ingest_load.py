
import os
import logging
import datetime
from dotenv import load_dotenv
import boto3
import json
import argparse

from data_platform.db import dbSession
from data_platform.db.models.cubic_ods_table import CubicODSTable
from data_platform.db.models.cubic_ods_load import CubicODSLoad


# get enviroment variables from .env file, if not already set
load_dotenv()

def run():
  # initialize boto clients
  stepFunctions = boto3.client('stepfunctions')

  # to keep track of where the load objects will be appended into
  tables = {}

  with dbSession() as db:
    # get load records with 'ready' status, ordered by modified, key (just in case)
    loadRecs = db.query(CubicODSLoad, CubicODSTable)\
                 .join(CubicODSTable, CubicODSLoad.table_id == CubicODSTable.id)\
                 .filter(CubicODSLoad.status == 'ready')\
                 .order_by(CubicODSLoad.s3_modified, CubicODSLoad.s3_key)\
                 .all()
    # put them into a structure that makes sense as a step function input
    for loadRec, tableRec in loadRecs:
      if tableRec.name in tables:
        tables[tableRec.name]['load_ids'].append(loadRec.id)
        tables[tableRec.name]['load_s3_keys'].append('s3://{}/{}'.format(os.environ.get('S3_BUCKET_INCOMING'), loadRec.s3_key))
      else:
        tables[tableRec.name] = {
          'name': tableRec.name,
          's3_prefix': tableRec.s3_prefix,
          'snapshot': tableRec.snapshot.strftime("%Y%m%d%H%M%S"),

          'load_ids': [ loadRec.id ],
          'load_s3_keys': [ 's3://{}/{}'.format(os.environ.get('S3_BUCKET_INCOMING'), loadRec.s3_key) ]
        }

    # start ingesting workflow
    for table in tables.values():
      try:
        # update load records' status to 'ingesting'
        # note: setting modified explicitly since before_update in base.py doesn't run
        db.query(CubicODSLoad).filter(CubicODSLoad.id.in_(table.get('load_ids'))).update({
          CubicODSLoad.status: 'ingesting',
          CubicODSLoad.modified: datetime.datetime.utcnow()
        })
        db.commit()

        # construct input for step function
        stepFunctionInput = {
          'env': json.dumps({
            'GLUE_DATABASE_NAME': os.environ.get('GLUE_DATABASE_NAME'),
            'S3_BUCKET_SPRINGBOARD': os.environ.get('S3_BUCKET_SPRINGBOARD'),
            'S3_BUCKET_SPRINGBOARD_PREFIX': os.environ.get('S3_BUCKET_SPRINGBOARD_PREFIX'),
          }),
          'input': json.dumps({
            'tables': [ table ]
          })
        }
        # if on local environment, provide commands for workflow
        if os.environ.get('ENV') == 'local':
          logging.info('@todo')
        else:
          # run step function
          stepFunctions.start_execution(
            stateMachineArn=os.environ.get('INGEST_STEP_FUNCTION_ARN'),
            input=json.dumps(stepFunctionInput)
          )

        # commit 'ingesting' status change
        db.commit()

      # if execution failed to start, update status to 'ingesting_error', and log error
      except (
        stepFunctions.exceptions.ExecutionLimitExceeded,
        stepFunctions.exceptions.ExecutionAlreadyExists,
        stepFunctions.exceptions.InvalidArn,
        stepFunctions.exceptions.InvalidExecutionInput,
        stepFunctions.exceptions.InvalidName,
        stepFunctions.exceptions.StateMachineDoesNotExist,
        stepFunctions.exceptions.StateMachineDeleting
      ) as e:
        # note: setting modified explicitly since before_update in base.py doesn't run
        db.query(CubicODSLoad).filter(CubicODSLoad.s3_key.in_(table.get('load_ids'))).update({
          CubicODSLoad.status: 'ingesting_error',
          CubicODSLoad.modified: datetime.datetime.utcnow()
        })
        db.commit()

        logging.error('[data_platform] [batch] [cubic_ods] [ingest_load]: {}'.format(e))

if __name__ == '__main__':
  run()


