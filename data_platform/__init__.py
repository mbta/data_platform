
import os
import logging
from dotenv import load_dotenv


# get enviroment variables from .env file, if not already set
load_dotenv()

# configure logging
logLevel = logging.ERROR
if os.environ.get('ENV', '') == 'local':
  logLevel = logging.INFO
# config
logging.basicConfig(
  format='%(asctime)s %(levelname)s %(message)s',
  level=logLevel
)
