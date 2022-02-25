import Config

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

config :ex_cubic_ods_ingestion,
  ecto_repos: [ExCubicOdsIngestion.Repo],
  run_migrations_at_startup?: false

config :ex_cubic_ods_ingestion,
  start_app?: true

config :ex_cubic_ods_ingestion, Oban,
  repo: ExCubicOdsIngestion.Repo,
  plugins: [],
  queues: [
    ingest: 5
  ]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"
