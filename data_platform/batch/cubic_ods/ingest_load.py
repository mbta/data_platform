
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

def startStepFunctionExecution(botoClient, db, tables):
  # get load ids we are updating
  loadIds = []
  for table in tables:
    loadIds += table.get('load_ids', [])

  try:
    # if on local environment, provide commands for workflow
    if os.environ.get('ENV') == 'local':
      logging.info('@todo')
    else:
      # run step function
      botoClient.start_execution(
        stateMachineArn=os.environ.get('STEP_FUNCTION_INGEST_ARN'),
        input=json.dumps({
          'env': json.dumps({
            'GLUE_DATABASE_NAME': os.environ.get('GLUE_DATABASE_NAME'),
            'S3_BUCKET_SPRINGBOARD': os.environ.get('S3_BUCKET_SPRINGBOARD'),
            'S3_BUCKET_SPRINGBOARD_PREFIX': os.environ.get('S3_BUCKET_SPRINGBOARD_PREFIX'),
          }),
          'input': json.dumps({
            'tables': tables
          })
        })
      )

    # update load records' status to 'ingesting'
    # note: setting modified explicitly since before_update in base.py doesn't run. @todo why?
    db.query(CubicODSLoad).filter(CubicODSLoad.id.in_(loadIds)).update({
      CubicODSLoad.status: 'ingesting',
      CubicODSLoad.modified: datetime.datetime.utcnow()
    })
    # commit 'ingesting' status update
    db.commit()

  # if execution failed to start, update status to 'ingesting_error' for the tables, and log error
  except (
    botoClient.exceptions.ExecutionLimitExceeded,
    botoClient.exceptions.ExecutionAlreadyExists,
    botoClient.exceptions.InvalidArn,
    botoClient.exceptions.InvalidExecutionInput,
    botoClient.exceptions.InvalidName,
    botoClient.exceptions.StateMachineDoesNotExist,
    botoClient.exceptions.StateMachineDeleting
  ) as e:
    # note: setting modified explicitly since before_update in base.py doesn't run. @todo why?
    db.query(CubicODSLoad).filter(CubicODSLoad.s3_key.in_(loadIds)).update({
      CubicODSLoad.status: 'ingesting_error',
      CubicODSLoad.modified: datetime.datetime.utcnow()
    })
    # commit 'ingesting_error' status update
    db.commit()

    logging.error('{}: {}'.format(logPrefix, e))

def run():
  # initialize boto clients
  stepFunctionsClient = boto3.client('stepfunctions')

  # helpful variables
  # log prefix, for identifying logs
  logPrefix = '[data_platform] [batch] [cubic_ods] [ingest_load]'
  # data limit (in bytes), how many bytes allowed per execution
  byteLimit = 100000000 # 100MB
  # to keep track of where the load objects will be appended into
  tables = []
  # number of allowed executions. this number should align with the
  # concurrency value of the glue job.
  executionLimit = 10

  # get number of step function execution currently running, so we can adjust limit
  if os.environ.get('ENV') == 'local':
    logging.info('{}: No limitations on local.'.format(logPrefix))
  else:
    try:
      listExecutionsResponse = stepFunctionsClient.list_executions(
        stateMachineArn=os.environ.get('STEP_FUNCTION_INGEST_ARN'),
        statusFilter='RUNNING',
        # use execution limit
        maxResults=executionLimit # max allowed is 1000
      )

      # adjust execution limit based on the number currently running
      executionLimit = executionLimit - len(listExecutionsResponse.get('executions', []))
    except (
      stepFunctionsClient.exceptions.InvalidArn,
      stepFunctionsClient.exceptions.InvalidToken,
      stepFunctionsClient.exceptions.StateMachineDoesNotExist,
      stepFunctionsClient.exceptions.StateMachineTypeNotSupported
    ) as e:
      logging.error('{}: {}'.format(logPrefix, e))

  with dbSession() as db:
    # get load records with 'ready' status, ordered by modified, key (just in case)
    loadRecs = db.query(CubicODSLoad, CubicODSTable)\
                 .join(CubicODSTable, CubicODSLoad.table_id == CubicODSTable.id)\
                 .filter(CubicODSLoad.status == 'ready')\
                 # get records that are older than 30 minutes ago, giving time s3 events/lambda to process
                 # .filter(CubicODSLoad.s3_modified < (datetime.datetime.utcnow() - datetime.timedelta(minutes=30)))\
                 .order_by(CubicODSLoad.s3_modified, CubicODSLoad.s3_key)\
                 .all()

    # put them into a structure that makes sense as a step function input
    for loadRec, tableRec in loadRecs:
      # for cdc loads, we always have one table and one load. otherwise Spark
      # will load them into one data frame, and output into one parquet file
      if loadRec.is_cdc:
        tables.append({
          'name': tableRec.name,
          's3_prefix': tableRec.s3_prefix,
          'snapshot': tableRec.snapshot.strftime("%Y%m%d%H%M%S"),

          'load_is_cdc': True,
          'load_ids': [ loadRec.id ],
          'load_s3_keys': [ 's3://{}/{}'.format(os.environ.get('S3_BUCKET_INCOMING'), loadRec.s3_key) ],
          'load_bytes': loadRec.s3_size
        })

      # for regular loads, they can be appended into one Spark data frame so append them
      else:
        # check to see if the table has already been added for ingestion
        tableAdded = False
        for table in tables:
          # if table added, just append the load info
          if table.get('name') == tableRec.name && table.get('load_is_cdc', False):
            table['load_ids'].append(loadRec.id)
            table['load_s3_keys'].append('s3://{}/{}'.format(os.environ.get('S3_BUCKET_INCOMING'), loadRec.s3_key))
            table['load_bytes'] += loadRec.s3_size # add to content size

            tableAdded = True
            break

        # if table not added, add it with the load info
        if not tableAdded:
          tables.append({
            'name': tableRec.name,
            's3_prefix': tableRec.s3_prefix,
            'snapshot': tableRec.snapshot.strftime("%Y%m%d%H%M%S"),

            'load_ids': [ loadRec.id ],
            'load_s3_keys': [ 's3://{}/{}'.format(os.environ.get('S3_BUCKET_INCOMING'), loadRec.s3_key) ],
            'load_bytes': loadRec.s3_size
          })

    # start ingesting workflow
    executionCount = 0
    byteCount = 0
    stepFunctionInputTables = []
    for table in tables.values():
      # update byte count
      byteCount += table.get('load_bytes', 0)

      # add to the step function input
      stepFunctionInputTables.append(table)

      # if we have reached the byte limit, then start an execution
      if byteCount >= byteLimit:
        # start execution only if we haven't reached the limit
        if executionCount <= executionLimit:
          startStepFunctionExecution(stepFunctionsClient, db, stepFunctionInputTables)

          # add to execution count to allow for comparing to limit
          executionCount += 1

          # reset
          byteCount = 0
          stepFunctionInputTables = []

    # if there are leftover tables to be ingested, start one last execution
    if stepFunctionInputTables:
      # start execution only if we haven't reached the limit
      if executionCount <= executionLimit:
        startStepFunctionExecution(stepFunctionsClient, db, stepFunctionInputTables)

if __name__ == '__main__':
  run()
