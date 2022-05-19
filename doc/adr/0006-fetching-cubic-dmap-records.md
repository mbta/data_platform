
# 6. Implementation Proposal For Fetching DMAP Records

Date: 2022-05-19

## Status

Draft

## Context

Cubic is implementing interfaces for presenting some ODS data in a de-personalized way. A subset of this data is also aggregated to be presented in reports pre-determined by Cubic/MBTA.

CTD has been tasked with fetching this data (DMAP) and ingesting it into the MBTA's data lake. We have refactored the Data Platform's Cubic Ingestion process to allow for ingesting DMAP data, but we still need to implement the fetching of this data from Cubic's cloud storage.

This ADR will present these implementation details.

## Assumptions

DMAP data is organized into feeds. We will be polling these feeds with a specific timeframe to get only one set of data for each feed for that timeframe. The feeds are expected to be provided by Cubic through the same API, allowing for reuse of the fetching code for each feed.

The feed will also be updated daily, so there will not be a need to constantly fetch more than on a daily basis.

On the infrastructure side, we will reuse the Data Platform always-on container to scheduel/run the jobs for fetching these dataset.

## Implemetation

### Code

We will create 2 new Oban workers within the ExCubicIngestion Elixir application.

#### Schedule DMAP

This worker will be triggered every morning (exact time TBD) by the [Oban Cron plugin](https://hexdocs.pm/oban/Oban.Plugins.Cron.html). It won't receive any arguments and will do the following:

1. Determine the timeframe (`start_date` and `end_date`) to request data for.
2. Use a database transaction to insert `Fetch DMAP` Oban jobs for each feed, passing this information about the feed:
    - URL
    - Start Date
    - End Date

#### Fetch DMAP

This worker will be triggered but the `Schedule DMAP` job, but can also be triggered by other processes, such as a one-off script to refetch data for a previous timeframe. It will be perfoming the following steps:

1. Request the `args.url` of the feed with the `args.start_date` and `args.end_date` as parameters.
2. For each item in the "results":
    - Validate the start date requested is less than or equal to the start date of the result.
    - Validate the end date requested is more than or equal to the end date of the result.
    - Upon validation, append the result to list for further processing.
3. Loop through the compiled list results and for each `result`:
    - Store information about it in the database as a record in the `cubic_dmap_datasets` table, with this mapping:
        * `result.id` => type
        * `result.dataset_id` => identifier
        * `result.start_data` => start_date
        * `result.end_data` => end_date
        * `result.last_updated` => last_updated
    - Request the `result.url` and store file locally as `{result.dataset_id}.csv.gz`
    - Upload the file to the 'Incoming' bucket, under the `cubic/dmap/{result.id}/` prefix.
    - Delete the file from local.

### Database

A new table will be created to store DMAP dataset information.

**cubic_dmap_datasets**  

Fields:  
- status  
- type  
- identifier  
- start_date  
- end_date  
- last_updated  


### Infrastructure

Update IAM for Data Platform's ECS container to be able to write to the 'Incoming' bucket.

## Consequences

Scheduled tasks have traditionally been at the system level. Relying on an application that is managing other processes, in addition to scheduling the fetching of DMAP data, has risk. The overburdening of the system should have us consider more power for the container, but this should be evaluated at the time of deployment.
