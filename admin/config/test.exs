import Config

# only log warnings+ in test
config :logger, level: :info

config :admin, Admin.Repo, pool: Ecto.Adapters.SQL.Sandbox
