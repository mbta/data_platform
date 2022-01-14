
import json
import logging
import argparse

from data_platform.lambdas.cubic_ods import process_incoming


def run(event, context):
  # run lambda on the specific key that triggered the eventbridge event
  return process_incoming.run(event.get('detail', {}).get('requestParameters', {}).get('key'))

if __name__ == '__main__':
  # use argparse to make modular and run this on local
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--event',
    help='Ex. --event \"{\\\"detail\\\": {\\\"requestParameters\\\": {\\\"key\\\": \\\"cubic_ods_qlik/EDW.SAMPLE/LOAD1.csv\\\"}}}\"',
    required=True
  )
  args = parser.parse_args()

  # run
  run(json.loads(args.event), None)
