
import Config

config :ex_aws,
  # note: 'region' doesn't work. it's inherited from the profile
  # region: [{:system, "AWS_REGION"}, {:awscli, "default", 30}, :instance_role],
  access_key_id: [{:system, "AWS_ACCESS_KEY_ID"}, {:awscli, "default", 30}, :instance_role],
  secret_access_key: [{:system, "AWS_SECRET_ACCESS_KEY"}, {:awscli, "default", 30}, :instance_role]

config :ex_cubic_ods_ingestion,
  s3_prefix_operations: System.get_env("S3_PREFIX_OPERATIONS"),
  s3_prefix_incoming: System.get_env("S3_PREFIX_INCOMING"),
  s3_prefix_archive: System.get_env("S3_PREFIX_ARCHIVE"),
  s3_prefix_error: System.get_env("S3_PREFIX_ERROR"),
  s3_prefix_springboard: System.get_env("S3_PREFIX_SPRINGBOARD"),

  s3_bucket_operations: System.get_env("S3_BUCKET_OPERATIONS"),
  s3_bucket_incoming: System.get_env("S3_BUCKET_INCOMING"),
  s3_bucket_archive: System.get_env("S3_BUCKET_ARCHIVE"),
  s3_bucket_error: System.get_env("S3_BUCKET_ERROR"),
  s3_bucket_springboard: System.get_env("S3_BUCKET_SPRINGBOARD")

config :ex_cubic_ods_ingestion, ExCubicOdsIngestion.Repo,
  username: System.get_env("DB_USER"),
  database: System.get_env("DB_NAME"),
  hostname: System.get_env("DB_HOST"),
  password: System.get_env("DB_PASSWORD"),
  port: "DB_PORT" |> System.get_env("5432") |> String.to_integer(),
  configure: {ExCubicOdsIngestion.Repo, :before_connect, []}
