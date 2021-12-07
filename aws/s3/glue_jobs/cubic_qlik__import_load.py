
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from data_platform.jobs.cubic_qlik import import_load

# initiate session
spark = SparkSession.builder.getOrCreate()

# run job
import_load.run(spark)
