import Config

# only log warnings+ in test
config :logger, level: :info

config :ex_cubic_ingestion,
  start_app_children?: false

config :ex_cubic_ingestion, Oban, queues: false, plugins: false

config :ex_cubic_ingestion, ExCubicIngestion.Repo,
  pool: Ecto.Adapters.SQL.Sandbox,
  timeout: 60_000
