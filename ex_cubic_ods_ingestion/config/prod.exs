import Config

config :ex_cubic_ods_ingestion, ExCubicOdsIngestion.Repo,
  show_sensitive_data_on_connection_error: false,
  ssl: true,
  use_iam_token: true

config :ex_cubic_ods_ingestion,
  run_migrations_at_startup?: true
