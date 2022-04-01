import Config

config :ex_cubic_ods_ingestion,
  s3_bucket_operations: System.get_env("S3_BUCKET_OPERATIONS", ""),
  s3_bucket_prefix_operations: System.get_env("S3_BUCKET_PREFIX_OPERATIONS", ""),
  s3_bucket_incoming: System.get_env("S3_BUCKET_INCOMING", ""),
  s3_bucket_prefix_incoming: System.get_env("S3_BUCKET_PREFIX_INCOMING", ""),
  s3_bucket_archive: System.get_env("S3_BUCKET_ARCHIVE", ""),
  s3_bucket_prefix_archive: System.get_env("S3_BUCKET_PREFIX_ARCHIVE", ""),
  s3_bucket_error: System.get_env("S3_BUCKET_ERROR", ""),
  s3_bucket_prefix_error: System.get_env("S3_BUCKET_PREFIX_ERROR", ""),
  s3_bucket_springboard: System.get_env("S3_BUCKET_SPRINGBOARD", ""),
  s3_bucket_prefix_springboard: System.get_env("S3_BUCKET_PREFIX_SPRINGBOARD", ""),
  glue_database_incoming: System.get_env("GLUE_DATABASE_INCOMING", ""),
  glue_database_springboard: System.get_env("GLUE_DATABASE_SPRINGBOARD", ""),
  glue_job_cubic_ods_ingest: System.get_env("GLUE_JOB_CUBIC_ODS_INGEST", "")
