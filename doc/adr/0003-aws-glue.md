# 3. AWS Glue

Date: 2022-02-02

## Status

Accepted

## Context

We need to process CSV files written to S3, into more efficient formats.

## Decision

We will use AWS Glue (based on Spark) scripts written in Python to write Parquet files.

Glue is serverless, which doesn't require us maintaining infrastructure the way
Elastic Map Reduce does. It also supports basic reliability functionality such
as timeouts and retries.

Python is well-known to our engineers, more than Java/Scala which is the
alternative Glue/Spark language.

Parquet is well-supported by Spark (for reading/writing) and also Athena
(querying), is a binary format (more efficient), and supports all the relevant
data types.

## Consequences

This makes the container language ([ADR#0002](0002-elixir-ecs.md)) separate from the the data
processing language. However, we feel that this is a reasonable trade-off to
keep our container infrastructure consistent.
