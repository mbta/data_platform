import Config

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

config :ex_cubic_ingestion,
  ecto_repos: [ExCubicIngestion.Repo],
  run_migrations_at_startup?: false,
  start_app_children?: true

config :ex_cubic_ingestion, Oban,
  repo: ExCubicIngestion.Repo,
  plugins: [
    # {
    #   Oban.Plugins.Cron,
    #   crontab: [
    #     {"0 15 * * *", ExCubicIngestion.Workers.ScheduleDmap, max_attempts: 1}
    #   ]
    # }
  ],
  queues: [
    archive: 1,
    error: 1,
    fetch_dmap: 1,
    ingest: 5,
    schedule_dmap: 1
  ]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"
