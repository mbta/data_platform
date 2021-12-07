
import importlib

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from test_kot import app
# initiate session (per submit)
spark = SparkSession.builder.getOrCreate()

# helpful variables
schema_json = '{"fields":[{"metadata":{},"name":"SAMPLE_ID","nullable":true,"type":"integer"},{"metadata":{},"name":"SAMPLE_NAME","nullable":true,"type":"string"},{"metadata":{},"name":"EDW_INSERTED_DTM","nullable":true,"type":"timestamp"},{"metadata":{},"name":"EDW_UPDATED_DTM","nullable":true,"type":"timestamp"}],"type":"struct"}'
# csvPath = 'sample_data/cubic_qlik/EDW.SAMPLE/LOAD1.csv'
csvPath = 'sample_data/cubic_qlik/EDW.SAMPLE__ct/20211201-112233444.csv'
parquetPath = '{}.parquet'.format(csvPath)
tableName = 'data'

# load up CSV into a dataframe and infer a schema
csvDF = spark.read.option('inferSchema', 'true').csv(csvPath, header=True)
print('##################################')
print(csvDF.schema.json())
print('##################################')

# write to parquet format
# csvDF.write.partitionBy(...).parquet(parquetPath)
csvDF.write.mode('overwrite').parquet(parquetPath)

# read parquet data
parquetDF = spark.read.parquet(parquetPath)
print('##################################')
print(parquetDF.schema.json())
print('##################################')

# #############################################################
# write to table
parquetDF.createOrReplaceTempView(tableName)

# do some reading of data
tempDF = spark.sql("select * from {} where SAMPLE_ID = '{}'".format(tableName, 4))
tempDF.show()

# testWhl = importlib.import_module('test_kot')
app.blah()
