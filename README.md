

# Local

### Setup

(not tested, just notes)

1. `asdf plugin-add python`
1. `adsf plugin-add adr-tools`
1. `asdf install`
1. `pip install virtualenv`
1. `python -m venv venv`
1. `source venv/bin/activate`
1. `pip install -r requirements`

### Docker

To build and stand up the database, glue, and the main containers:
```sh
# start docker, and then
docker-compose up
```

To run migrations:
```sh
docker-compose run --rm main__local alembic upgrade head
```

To login into database:
```sh
# assuming `docker-compose up`
docker exec -it db__local bash
# in docker bash
psql -U postgres -d data_platform
```

To run glue jobs:
```sh
# ex.
docker-compose run --rm glue__local /glue/bin/gluesparksubmit /data_platform/aws/s3/glue_jobs/{glue_script_name}.py --JOB_NAME {glue_job_name} [--OBJECT_KEY {s3_object_key}]
```

To run batch jobs:
```sh
# ex.
docker-compose run --rm main__local python -m data_platform.batch.cubic_qlik.process_new_table --table {table_name}
```


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

1. ...


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

### aws

### data_platform
### data_platform/batch
### data_platform/db
### data_platform/db/cert
### data_platform/db/versions
### data_platform/jobs

### docker

### sample_data

### tests
