# 2. Elixir / ECS

Date: 2022-02-02

## Status

Accepted

## Context

In the MVP of the architecture, we have an application running continuously, to:

- monitor S3 for new files
- create Glue jobs to process the files
- monitor Glue jobs to ensure we don't run too many

## Decision

This application will be written in Elixir and run in ECS. This matches our other applications which communicate with AWS services (such as Concentrate and RTR).

## Consequences

There will be a ramp-up period while engineers get familiar with Elixir. However, it allows us to reuse our existing infrastructure (CI/CD) rather than needing to reimplement.
