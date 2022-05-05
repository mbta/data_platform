import Config

config :ex_cubic_ingestion, ExCubicIngestion.Repo,
  show_sensitive_data_on_connection_error: false,
  ssl: true,
  use_iam_token: true

config :ex_cubic_ingestion,
  run_migrations_at_startup?: true

config :ex_aws,
  # overwrite defaults here, so as to only look at instance role
  access_key_id: :instance_role,
  secret_access_key: :instance_role
