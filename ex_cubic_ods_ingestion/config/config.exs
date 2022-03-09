import Config

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

config :ex_cubic_ods_ingestion,
  ecto_repos: [ExCubicOdsIngestion.Repo],
  run_migrations_at_startup?: false,
  start_app?: true

config :ex_cubic_ods_ingestion, Oban,
  repo: ExCubicOdsIngestion.Repo,
  plugins: [],
  queues: [
    archive: 5,
    error: 5,
    ingest: 5
  ]

config :ex_cubic_ods_ingestion, ExCubicOdsIngestion.Repo,
  username: System.get_env("DB_USER"),
  database: System.get_env("DB_NAME"),
  hostname: System.get_env("DB_HOST"),
  password: System.get_env("DB_PASSWORD"),
  port: "DB_PORT" |> System.get_env("5432") |> String.to_integer(),
  configure: {ExCubicOdsIngestion.Repo, :before_connect, []}

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"
