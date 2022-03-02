import Config

# only log warnings in test
config :logger, level: :warn

config :ex_cubic_ods_ingestion, ExCubicOdsIngestion.Repo,
  database: "#{System.get_env("DB_NAME")}_test",
  pool: Ecto.Adapters.SQL.Sandbox

config :ex_cubic_ods_ingestion,
  start_app?: false

config :ex_cubic_ods_ingestion, Oban, queues: false, plugins: false
