import Config

config :admin, Admin.Repo,
  username: System.get_env("DB_USER"),
  database: System.get_env("DB_NAME"),
  hostname: System.get_env("DB_HOST"),
  password: System.get_env("DB_PASSWORD"),
  port: "DB_PORT" |> System.get_env("5432") |> String.to_integer(),
  configure: {Admin.Repo, :before_connect, []},
  # default to 10
  pool_size: "DB_POOL_SIZE" |> System.get_env("10") |> String.to_integer()

# for testing, we'd like to change the database
if config_env() == :test do
  config :admin, Admin.Repo, database: "#{System.get_env("DB_NAME")}_test"
end
