
import json
import logging
import argparse

from data_platform.lambdas.cubic_ods import process_ingestion


def run(event, context):
  # run lambda with the input of the glue job
  return process_ingestion.run(event.get('glue_job_input', {}))

if __name__ == '__main__':
  # use argparse to make modular and run this on local
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--event',
    help='Ex. --event \"{\\\"glue_job_input\\\": {}}\"',
    required=True
  )
  args = parser.parse_args()

  # run
  run(json.loads(args.event), None)
