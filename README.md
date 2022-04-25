

# Local

### Setup

Run the following:
```sh
asdf plugin-add adr-tools
asdf plugin-add elixir
asdf plugin-add erlang
asdf plugin-add poetry
asdf plugin-add python
asdf plugin-add terraform
asdf install
```

### Docker

To build and stand up the database and glue containers:
```sh
# start docker, and then
docker-compose up
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
docker-compose run --rm glue__local /glue/bin/gluesparksubmit /data_platform/aws/s3/glue_jobs/{glue_script_name}.py --JOB_NAME {glue_job_name} [--ARGS "..."]
```


# Folder Structure

### aws

The `s3/` folder within this folder contains the files that will be synced up to S3 during a `glue-python-deploy` CI run. Additionally the `s3/glue_jobs/` contains the glue jobs' code as it will be run by AWS Glue.

### doc

The `adr/` here contains the the various architectural decisions made over the course of the Data Platform's development. Further documentation can be in [Notion](https://www.notion.so/mbta-downtown-crossing/Data-Platform-9f78ea9ad675432c87ab08d6d38280c2).

### docker

Contains docker files that are used for local development of the Data Platform. These docker are separate from applications that operate various parts of the Data Platform.

### ex_cubic_ods_ingestion

An Elixir application that runs the ODS ingestion process. Further documentation can be in [Notion](https://www.notion.so/mbta-downtown-crossing/Data-Platform-9f78ea9ad675432c87ab08d6d38280c2).

### py_cubic_ods_ingestion

A python package to hold all of the `cubic_ods_ingest_incoming` Glue job code, including tests and package requirements.

### sample_data

Sample data that is similar in structure to what we currently have coming into the 'Incoming' S3 bucket.

### terraform

A space for engineer's to create infrastructure that support local development. See [README](https://github.com/mbta/data_platform/blob/main/terraform/README.md).

# Links

* [Architecture Designs (Miro)](https://miro.com/app/board/o9J_liWCxTw=/)
* [Notion](https://www.notion.so/mbta-downtown-crossing/Data-Platform-9f78ea9ad675432c87ab08d6d38280c2)

