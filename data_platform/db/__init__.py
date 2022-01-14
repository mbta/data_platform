
import logging
import boto3
import os
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

# import tables to put them in scope and allow alembic to autogenerate any changes
from data_platform.db.models import (
  cubic_ods_table,
  cubic_ods_load
)


# get enviroment variables from .env file, if not already set
load_dotenv()

# helper variables for interacting with database
dbHost = os.environ.get('DB_HOST')
dbPort = os.environ.get('DB_PORT', '5432')
dbUser = os.environ.get('DB_USER')
dbPassword = os.environ.get('DB_PASSWORD')
dbName = os.environ.get('DB_NAME')
# connection url to database
dbURL = ''

connectArgs = {}
# if we are on local, then make some updates
if os.environ.get('ENV') == 'local':
  # a couple of options for local based on whether user/password is set or not
  if dbUser and dbPassword:
    dbURL = 'postgresql+psycopg2://{}:{}@{}/{}'.format(dbUser, dbPassword, dbHost, dbName)
  else:
    dbURL = 'postgresql+psycopg2://{}/{}'.format(dbHost, dbName)

# otherwise running on RDS
else:
  # initialize clients
  rds = boto3.client('rds')

  # generate IAM-auth password
  dbPassword = rds.generate_db_auth_token(DBHostname=dbHost,Port=dbPort,DBUsername=dbUser)

  dbURL = 'postgresql+psycopg2://{}:{}@{}/{}?sslmode={}&sslrootcert={}'.format(
    dbUser,
    urllib.parse.quote_plus(dbPassword),
    dbHost,
    dbName,
    'verify-full',
    './data_platform/db/certs/AmazonRootCA1.pem'
  )

# create connection
dbEngine = create_engine(dbURL, connect_args=connectArgs)

# database session maker
dbSession = sessionmaker(dbEngine)
