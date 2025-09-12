

# Local

### Setup

Run the following:
```sh
asdf plugin add elixir
asdf plugin add erlang
asdf install
```

### Environment

**Note:** Some local, but sensitive, information is stored in 'App: Data Platform' 1Password Vault.

Please copy `.env.template` to `.env` and make the following updates.

Replace `{s3_bucket}` with a S3 bucket you have access to. The Data Platform team has a default one, so feel free to ask what it is and how to get access to it.

Replace `{username}` with your AWS username, ex. `ggjura`.

```
# buckets
S3_BUCKET_OPERATIONS={s3_bucket}
S3_BUCKET_INCOMING={s3_bucket}
S3_BUCKET_ARCHIVE={s3_bucket}
S3_BUCKET_ERROR={s3_bucket}
S3_BUCKET_SPRINGBOARD={s3_bucket}
# prefixes
S3_BUCKET_PREFIX_OPERATIONS={username}/operations/
S3_BUCKET_PREFIX_INCOMING={username}/incoming/
S3_BUCKET_PREFIX_ARCHIVE={username}/archive/
S3_BUCKET_PREFIX_ERROR={username}/error/
S3_BUCKET_PREFIX_SPRINGBOARD={username}/springboard/
```

If you have setup a local infrastructure (see [this](https://github.com/mbta/data_platform/blob/main/terraform/README.md)), then you can update the following accordingly.

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

### App: ex_cubic_ingestion

Run the following to allow for this application to run locally:

```sh
cd ex_cubic_ingestion
mix deps.get
mix ecto.migrate
```

You should then be able to run the application with:
```sh
iex -S mix
```

# Folder Structure

### aws

The `s3/` folder within this folder contains the files that will be synced up to S3 during a `glue-python-deploy` CI run. Additionally the `s3/glue_jobs/` contains the glue jobs' code as it will be run by AWS Glue.

### doc

The `adr/` here contains the the various architectural decisions made over the course of the Data Platform's development. Further documentation can be found in [Notion](https://www.notion.so/mbta-downtown-crossing/Data-Platform-9f78ea9ad675432c87ab08d6d38280c2).

### docker

Contains docker files that are used for local development of the Data Platform. These docker are separate from applications that operate various parts of the Data Platform.

### ex_cubic_ingestion

An Elixir application that runs the Cubic Ingestion process. Further documentation can be found in [Notion](https://www.notion.so/mbta-downtown-crossing/Data-Platform-9f78ea9ad675432c87ab08d6d38280c2).

### sample_data

Sample data that is similar in structure to what we currently have coming into the 'Incoming' S3 bucket.


# Links

* [Architecture Designs (Miro)](https://miro.com/app/board/o9J_liWCxTw=/)
* [Notion](https://www.notion.so/mbta-downtown-crossing/Data-Platform-9f78ea9ad675432c87ab08d6d38280c2)
