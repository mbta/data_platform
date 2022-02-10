import Config

config :ex_cubic_ods_ingestion,
  lib_ex_aws: ExCubicOdsIngestion.Mock.ExAws,
  lib_ex_aws_s3: ExCubicOdsIngestion.Mock.ExAws.S3

config :ex_cubic_ods_ingestion, ExCubicOdsIngestion.Repo, pool: Ecto.Adapters.SQL.Sandbox
