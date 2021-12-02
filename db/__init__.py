
import boto3
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

# import tables to put them in scope and allow alembic to autogenerate any changes
from db.models import (
  cubic_qlik_batch_load,
  cubic_qlik_cdc_load,
  cubic_qlik_table
)


# get enviroment variables from .env file
load_dotenv()

# helper variables for interacting with database
dbHost = os.environ.get('DB_HOST')
dbPort = os.environ.get('DB_PORT', '5432')
dbUser = os.environ.get('DB_USER')
dbPassword = os.environ.get('DB_PASSWORD')
dbSSL = {} # no config initially
dbName = os.environ.get('DB_NAME')
# connection url to database
dbURL = ''
# if we are on local, then make some updates
if os.environ.get('ENV') == 'local':
  # a couple of options for local based on whether user/password is set or not
  if dbUser and dbPassword:
    dbURL = 'postgresql://{}:{}@{}/{}'.format(dbUser, dbPassword, dbHost, dbName)
  else:
    dbURL = 'postgresql://{}/{}'.format(dbHost, dbName)

# otherwise running on RDS
else:
  # initialize clients
  rds = boto3.client('rds')

  # generate IAM-auth password
  dbPassword = rds.generate_db_auth_token(
    DBEndPoint=dbHost,
    Port=dbPort,
    DBUsername=dbUser
  )
  # rds provided pem file (see https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html)
  dbSSL = {
    'sslmode': 'require',
    'sslrootcert': 'cert/us-east-1-bundle.pem'
  }

  dbURL = 'postgresql://{}:{}@{}/{}'.format(dbUser, dbPassword, dbHost, dbName)

# set up a session to the database, and return it for use
def session():
  # create connection
  engine = create_engine(dbURL)

  # create session and return it
  return Session(engine)
