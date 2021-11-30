

create table metadata__tables (
  id bigint,
  name varchar,
  modified_field varchar,
)

create table metadata__full_loads (
  id bigint,
  table_id bigint,
  created datetime,
  modified datetime,

  -- load
  object_key varchar,
  size long bigint,
  status enum('available', 'processing', 'processed')

  -- process
  started datetime,
  completed datetime,

  -- info on rows
  rows_processed bigint,
  rows_max_modified datetime, -- the max modified from all the rows. if modified_field null, S3 object timestamp is used

);

create table metadata__ct_loads (
  id,
  table_id,
  created datetime,
  modified datetime,

  -- load
  object_key,
  size bigint,
  status enum('available', 'processing', 'processed')

  -- process
  started datetime,
  completed datetime,

  -- info on rows
  rows_processed bigint,
  rows_inserted bigint,
  rows_updated bigint,
  rows_deleted bigint,
  rows_max_modified datetime, -- the max modified from all the rows

);
