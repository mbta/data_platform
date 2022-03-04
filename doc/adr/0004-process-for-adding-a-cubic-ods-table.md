# 4. Process for adding a Cubic ODS table

Date: 2022-03-02

## Status

Pending

## Context

This documents the process (in particular, the ordering) of the steps needed to
have ExCubicOdsIngestion read and process a Cubic ODS table. In particular, it
does not need the table to be present in the Glue Catalog or the RDS database
before Cubic sends the table data to S3.

## Assumptions

- tables which are not configured in the RDS database are not scanned or imported

## Process

1. The table data in the Incoming bucket is scanned with a Glue Crawler, to discover the schema.
  - This should include a scan of both the bulk data prefix, as well as the change tracking `__ct` prefix.
1. Any changes to the schema (such as conversion of integer fields to strings) are made.
1. The Glue Catalog tables are described in Terraform and imported.
1. An `CubicOdsTable` record is created in the RDS database:
  - `name`: name of the Glue Catalog table, all lower case and with underscores (ex: `cubic_ods_qlik__edw_use_transaction`)
  - `s3_prefix`: the S3 prefix, including any application-specific prefix, without a trailing slash (ex: `cubic_ods_qlik/EDW.USE_TRANSACTION`)
  - `snapshot_s3_key`: the initial snapshot load file (ex: `cubic_ods_qlik/EDW.USE_TRANSACTION/LOAD00000001.csv.gz`)
1. Once the `CubicOdsTable` record is present in the RDS database, the `ExCubicOdsIngestion.ProcessIncoming` worker should pick up the new tables.

## Consequences

There are several manual steps here, of which some could be automated:

- Crawling the Incoming bucket with a Glue Crawler could be scheduled.
- A script/Mix task could be written to turn a Glue Catalog table into a Terraform configuration, for easier importing.
- A script/Mix task could be written to create a `CubicOdsTable`.
- A script/Mix task could be written to generate an Ecto data-only migration to add the `CubicOdsTable` record.

This also requires a developer to directly access the RDS database: a TODO for
the future is to either have this configuration outside the RDS database or
otherwise able to be updated without a direct database connection.

- Move the table configuration out of the RDS database and into a tool such as AWS Secrets Manager or the Glue Catalog.
- Create a web interface (requiring access control to this management tool)
