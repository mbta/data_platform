
import boto3
import os
import pg8000
from dotenv import load_dotenv


# set enviroment variables from .env file
load_dotenv()

# initialize clients (this uses the default session, i.e. instance profile/role)
s3 = boto3.client('s3')
rds = boto3.client('rds')
# if we have specified a boto profile to use, then override clients to use the specific session
if os.environ.get('BotoProfile'):
  session = boto3.Session(profile_name=os.environ.get('BotoProfile'))
  s3 = session.client('s3')
  rds = session.client('rds')

# helper variables for interacting with database
dbHost = os.environ.get('DBHost')
dbPort = os.environ.get('DBPort', '5432')
dbUser = os.environ.get('DBUser')
dbPassword = rds.generate_db_auth_token(
  DBEndPoint=dbHost,
  Port=dbPort,
  DBUsername=dbUser
)
dbName = os.environ.get('DBName')

# main function to run
def main(tablePrefix=None, dryRun=True):

  # establish database connection
  connection = pg8000.connect(
    host=dbHost,
    user=dbUser,
    database=dbName,
    password=dbPassword,
    # rds provided pem file (see https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html)
    ssl={
      'sslmode': 'require',
      'sslrootcert': 'cert/us-east-1-bundle.pem'
    }
  )

  # verify table is in our data platform

  # get all current objects available for import from s3 for the table
  # note: there could be hundreds or thousands of objects, so we paginate

  # get all import records for the table

  # remove objects that are already in the process of being imported














  if dryRun:
    pass
  else:
    pass

# script controller
if __name__ == '__main__':

  dryRun = False

  main(dryRun=dryRun)
