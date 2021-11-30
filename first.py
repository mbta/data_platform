
from pyspark.sql import SparkSession

# initiate session (per submit)
spark = SparkSession.builder.getOrCreate()


# helpful variables
csvPath = 'LOAD00000001.csv'
parquetPath = '{}.parquet'.format(csvPath)
tableName = 'data'

# load up CSV into a dataframe and infer a schema
csvDF = spark.read.option('inferSchema', 'true').csv(csvPath, header=True)

# write to parquet format
# csvDF.write.partitionBy(...).parquet(parquetPath)
csvDF.write.mode('overwrite').parquet(parquetPath)

# read parquet data
parquetDF = spark.read.parquet(parquetPath)
# write to table
parquetDF.createOrReplaceTempView(tableName)


# #############################################################

# do some reading of data
tempDF = spark.sql("select * from {} where DATE_KEY = '{}'".format(tableName, '20180507'))
tempDF.show()
