
# 5. Refactoring Ingestion to Account For DMAP/Kafka Data

Date: 2022-04-15

## Status

Draft

## Context

As more data streams come online from Cubic, we want to create uniformity in the ingestion process. This document provides an opportunity to discuss this implementation effort, and any drawbacks.

## Assumptions

This is how the different data streams will land in the ingestion pipeline, and how the various other architecture will be contructed to support it.

### Cubic ODS (Qlik)

Cubic will upload files to the 'Incoming' bucket directly, as controlled by their ODS/Qlik settings. The file paths will look like this:

* cubic/ods_qlik/EDW.SAMPLE/LOAD001.csv.gz
* cubic/ods_qlik/EDW.SAMPLE__ct/20220304-134556444.csv.gz

#### Glue

**Tables:**  
cubic__ods_qlik__edw_sample  
cubic__ods_qlik__edw_sample__ct  


#### Metadata Database

##### Tables

**cubic_tables**  
  name: 'cubic__ods_qlik__edw_sample'  
  s3_prefix: 'cubic/ods_qlik/EDW.SAMPLE/'  

**cubic_table_ods_snapshots**  
  table_id: {cubic_tables.id}  
  snapshot: {utc_datetime} # when data collection was started/restarted  
  snapshot_s3_key: 'cubic/ods_qlik/EDW.SAMPLE/LOAD001.csv.gz'  

**cubic_loads**  
  table_id: {cubic_tables.id}  
  status: 'ready', 'ingesting', 'ready_for_archiving', 'ready_for_erroring', 'archiving', 'erroring', 'archived', 'errored'  
  s3_key: {s3.object.key} # 'cubic/ods_qlik/EDW.SAMPLE/LOAD001.csv.gz' or 'cubic/ods_qlik/EDW.SAMPLE__ct/20220304-134556444.csv.gz'  
  s3_modified: {s3.object.modified}  
  s3_size: {s3.size}  

**cubic_load_ods_snapshots**  
  load_id: {cubic_loads.id}  
  snapshot: {cubic_table_ods_snapshots.snapshot} # note: at the time of creation  


### Cubic DMAP

The Python 'py_cubic_dmap_fetch' app will download files from Cubic's platform and upload them to the Incoming bucket every day. The files will contain a day's worth of data. The file paths will look like this:

* cubic/dmap/agg_sample/20220304.csv

#### Glue

**Tables:**  
cubic__dmap__agg_sample  

#### Metadata Database

##### Tables

**cubic_tables**  
  name: 'cubic__dmap__agg_sample'  
  s3_prefix: 'cubic/dmap/agg_sample/'  

**cubic_loads**  
  table_id: {cubic_tables.id}  
  status: 'ready', 'ingesting', 'ready_for_archiving', 'ready_for_erroring', 'archiving', 'erroring', 'archived', 'errored'  
  s3_key: {s3.object.key} # 'cubic/dmap/agg_sample/20220304.csv'  
  s3_modified: {s3.object.modified}  
  s3_size: {s3.size}  


### Cubic Kafka

**[TBD]**

One approach:  
A Kinesis Data Stream will receive Cubic's Kafka output. A Lambda will be triggered after 10,000 records or one hour has passed, whichever is first. The Lambda will create a file with these records and upload it to the Incoming bucket. The file paths will look like this:

* cubic/kafka/sample/vytxeTZskVKR7C7WgdSP3d.json # uuid


#### Glue

**Tables:**  
cubic__kafka__sample  

#### Metadata Database

##### Tables

**cubic_tables**  
  name: 'cubic__kafka__sample'  
  s3_prefix: 'cubic/kafka/sample/'  

**cubic_loads**  
  table_id: {cubic_tables.id}  
  status: 'ready', 'ingesting', 'ready_for_archiving', 'ready_for_erroring', 'archiving', 'erroring', 'archived', 'errored'  
  s3_key: {s3.object.key} # 'cubic/kafka/sample/20220304.csv'  
  s3_modified: {s3.object.modified}  
  s3_size: {s3.size}  


## Implemetation

Here we describe a path to implementation and refactoring of the current ingestion process.

### Metadata Database

**Tables**  
cubic_ods_tables -> cubic_tables  
cubic_ods_loads -> cubic_loads  
cubic_table_ods_snapshots (new)  
  table_id  
  snapshot  
  snapshot_s3_key  
cubic_load_ods_snapshots (new)  
  load_id  
  snapshot  

**Columns**  
cubic_tables.snapshot (drop)
cubic_tables.snapshot_s3_key (drop)
cubic_loads.snapshot (drop)

### ExCubicOdsIngestion -> ExCubicIngestion

#### Tests

Tests will need to be adjusted to be more specific to the `cubic/` prefix.

#### Schemas

`CubicOdsTable -> CubicTable` schema needs to be updated to drop `snapshot` and `snapshot_s3_key`.  
`CubicOdsLoad -> CubicLoad` schema needs to be updated to drop `snapshot`.  
`CubidTableOdsSnapshot` schema will be added.  
`CubidLoadOdsSnapshot` schema will be added.  

#### GenServers

`ProcessIncoming` will be updated to look under the `cubic/` prefix, when getting table prefixes from S3.

#### Comments

Comments should be updated to remove any callouts to 'ODS'.

### PyCubicOdsIngestion -> PyCubicIngestion

#### Glue Job

`cubic_ods_ingest_incoming.py -> cubic__ingest_incoming.py` will take into account ODS tables and writing with an additional 'snapshot' partition.

## Consequences

By making the ingestion process more generic, we risk:

* Getting locked into a specific implementation for all data streams.
* Having the ingestion stop for all streams if there is an issue in one.
* Introducing an extra step to the pipeline that might not be necessary for some data streams.




