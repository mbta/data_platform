
import boto3
import os
from dotenv import load_dotenv

import db


# get enviroment variables from .env file
load_dotenv()

# initialize clients (this uses the default session, i.e. instance profile/role)
s3 = boto3.client('s3')

# if we are on local, then make some updates
if os.environ.get('ENV') == 'local':

  # if we have specified a boto profile to use, then override clients to use the specific session
  if os.environ.get('BOTO_PROFILE'):
    session = boto3.Session(profile_name=os.environ.get('BOTO_PROFILE'))
    s3 = session.client('s3')

# main function to run
def main(tablePrefix=None, dryRun=True):

  # verify table is in our data platform

  # get all current objects available for import from s3 for the table
  # note: there could be hundreds or thousands of objects, so we paginate

  # get all import records for the table

  # remove objects that are already in the process of being imported

  # loop over objects that are left and run jobs














  if dryRun:
    pass
  else:
    pass

# script controller
if __name__ == '__main__':

  dryRun = False

  main(dryRun=dryRun)
