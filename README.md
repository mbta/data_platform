

# Local

Some learnings from setting up local environment.

* Spark (pyspark) 2.4.* requires Python 3.7.\* and JRE 8.
* The following needs to be in the ENV variables.
```
export JAVA_HOME=/usr/local/opt/openjdk@8
export SPARK_HOME=~/Development/external/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8
```

### Setup

(not tested, just notes)

1. `asdf install python 3.7`
2. `pip install virtualenv`
3. `source venv/bin/activate`

#### Things not answered:
* What does aws-glue-libs do? Seems to be just a wrapper around Spark.
* 


#### Things tried / failed / not worth it):
* Trying to install Spark through 'brew' (see [this](https://stackoverflow.com/questions/56601226/how-to-install-apache-spark-2-3-3-with-homebrew-on-mac))


# Spark

## Links

* https://medium.com/codefully-io/aws-glue-for-loading-data-from-a-file-to-the-database-extract-transform-load-fe6b722e11b8
* http://spark.apache.org/docs/latest/api/python/user_guide/index.html



# Schema

## DMAP

Objects are stored in Azure Blob Storage. Is accessed through a Apigee API. In AWS Glue, we will specify the schema manually.

### TODO

In Section 4.4.2 (Depersonalization Approach Revised Final Design (CDRL-DP-001-01:

* CDRL-DP-002-01, CDRL-DP-003-03).F.pdf), Get API key for Apigee from Cubic by opening up a ServiceNow ticket
* Build script to call API, receive JSON, extract Azure Blob storage URLs and download object.



## Qlik

Cubic will place 2 types of files for each data table in our S3 bucket. One is a snapshot (potentially large), and the other a change data capture (CDC) file containing changes to the snapshot.

Initially we will use the snapshot, to generate a catalog for the schemas of tables, and subsequently create new table (if it doesn't exist) in our data platform DB.

For subsequent snapshots, TBD.

On the CDC side, everytime a new object is placed in S3, we will trigger an EventBridge that triggers a AWS Glue Workflow that runs a AWS Glue job. The trigger will pass the object key to the job. The job will have these functions:

1. 


### Updating Parquet on Stage bucket
https://paulstaab.de/blog/2020/spark-dynamic-partition-overwrite.html
https://medium.com/nmc-techblog/spark-dynamic-partition-inserts-and-aws-s3-part-2-9ba0c97ad2c0



## Kafka

In AWS Glue, we will specify the schema manually, as crawling a stream is not possible.

```json
{
	"tapId": TAP_ID,
	"tokenId": TOKEN_ID,
	"stopId": STOP_ID,
	"deviceId": DEVICE_ID,
}
```

https://aws.amazon.com/about-aws/whats-new/2021/11/securely-connect-amazon-msk-clusters-over-internet/



# Not Reviewed / Groked

* Section 4 (ISD Data Warehouse Systems Final Design (CDRL-EF-002-06.02).C.pdf)





# Processes

## Adding New Table

1. File(s) from the new table have landed in S3.
2. Run a crawler to find schema of new table.
3. (optional) Work through AWS Glue DataBrew to study the type of data (maybe establish some transformations, such as PII)
4. Manually create AWS Glue Job to test import.
5. Create migration for new record identifying the table.
6. Create terraform script for AWS Glue Catalog for new table schema.



# Folder Structure

## async

## batch

## cert

## jobs

## lambdas

## migrations

## terraform

# Packages

* boto3
* pg8000 (used instead of psycopg2 because of BSD of license)


