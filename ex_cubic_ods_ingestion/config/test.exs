import Config

config :ex_cubic_ods_ingestion, ExCubicOdsIngestion.Repo, pool: Ecto.Adapters.SQL.Sandbox

config :ex_cubic_ods_ingestion,
  start_app?: false

# only log warnings in test
config :logger, level: :warn
