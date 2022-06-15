import Config

config :ex_cubic_ingestion, ExCubicIngestion.Repo,
  username: System.get_env("DB_USER"),
  database: System.get_env("DB_NAME"),
  hostname: System.get_env("DB_HOST"),
  password: System.get_env("DB_PASSWORD"),
  port: "DB_PORT" |> System.get_env("5432") |> String.to_integer(),
  configure: {ExCubicIngestion.Repo, :before_connect, []}

# for testing, we'd like to change the database
if config_env() == :test do
  config :ex_cubic_ingestion, ExCubicIngestion.Repo, database: "#{System.get_env("DB_NAME")}_test"
end

config :ex_cubic_ingestion,
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
  glue_job_cubic_ingestion_ingest_incoming:
    System.get_env("GLUE_JOB_CUBIC_INGESTION_INGEST_INCOMING", ""),
  dmap_base_url: System.get_env("CUBIC_DMAP_BASE_URL", ""),
  dmap_api_key: System.get_env("CUBIC_DMAP_API_KEY", "")
