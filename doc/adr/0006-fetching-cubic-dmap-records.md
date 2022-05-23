
# 6. Implementation Proposal For Fetching DMAP Records

Date: 2022-05-19

## Status

Draft

## Context

Cubic is implementing interfaces for presenting some ODS data in a de-personalized way. A subset of this data is also aggregated to be presented in reports pre-determined by Cubic/MBTA.

CTD has been tasked with fetching this data (DMAP) and ingesting it into the MBTA's data lake. We have refactored the Data Platform's Cubic Ingestion process to allow for ingesting DMAP data, but we still need to implement the fetching of this data from Cubic's cloud storage.

This ADR will present these implementation details.

## Assumptions

DMAP data is organized into feeds. We will be polling these feeds with the latest `last_updated` date/time that we have in the database for the feed. The feeds are expected to be provided by Cubic through the same API, allowing for reuse of the fetching code for each feed.

The feed are also be updated daily, so there will not be a need to constantly fetch more than on a daily basis.

On the infrastructure side, we will reuse the Data Platform always-on container to scheduel/run the jobs for fetching these dataset.

## Implemetation

### Code

We will create 2 new Oban workers within the ExCubicIngestion Elixir application.

#### Schedule DMAP

This worker will be triggered every morning (ideally after 14:30 UTC) by the [Oban Cron plugin](https://hexdocs.pm/oban/Oban.Plugins.Cron.html). It won't receive any arguments and will do the following:

1. Query the `cubic_dmap_feeds` table, and retrieve all active feeds.
2. Use a database transaction to insert `Fetch DMAP` Oban jobs for each feed, passing `feed.id` as an argument.

#### Fetch DMAP

This worker will be triggered but the `Schedule DMAP` job, but can also be triggered by other processes, such as a one-off script to refetch data.

Job arguments `args` should contain the following information:
* feed_id
* last_updated (optional)

It will be perfoming the following steps:

1. Get feed information from database by `args.feed_id`.
2. Construct the feed URL with `feed.relative_url` and `{args.last_updated || feed.last_updated} + 1ms`, and request.
3. For each item in the "results":
    - Validate that the `last_updated` is greater than `feed.last_updated`.
    - Upon validation, append the result to list for further processing.
4. Loop through the compiled list of results and for each `result`:
    - Upsert information as a record in the `cubic_dmap_datasets` table, with this mapping:
        * `feed.id` => feed_id
        * `result.id` => type
        * `result.dataset_id` => identifier
        * `result.start_data` => start_date
        * `result.end_data` => end_date
        * `result.last_updated` => last_updated
    - Request the `result.url` and store file contents in memory.
    - Upload the contents to the 'Incoming' bucket under the key `cubic/dmap/{result.id}/{result.dataset_id}.csv.gz`.

### Database

Two new tables will be created to store DMAP feed and dataset information.

**cubic_dmap_feeds**  

Contains information about the feed, including when the latest `last_updated` of its datasets (1-to-many relation).

Fields:  
- relative_url (ex. `/controlledresearchusersapi/transactional/device_event`)  
- last_updated  

**cubic_dmap_datasets**  

Contains information about the dataset as returned by Cubic's API.

Fields:  
- feed_id  
- type (ex. `device_event`)  
- identifier (ex. `device_event_20220517`)  
- start_date  
- end_date  
- last_updated  


### Infrastructure

Update IAM for Data Platform's ECS container to be able to write to the 'Incoming' bucket.

## Consequences

Scheduled tasks have traditionally been at the system level. Relying on an application that is managing other processes, in addition to scheduling the fetching of DMAP data, has risk. The overburdening of the system should have us consider more power for the container, but this should be evaluated closer to the time of deployment.
