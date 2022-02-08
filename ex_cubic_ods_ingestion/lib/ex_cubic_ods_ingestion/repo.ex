defmodule ExCubicOdsIngestion.Repo do
  use Ecto.Repo,
    otp_app: :ex_cubic_ods_ingestion,
    adapter: Ecto.Adapters.Postgres

  require Logger
  require ExAws.RDS

  @doc """
  Set via the `:configure` option in the ExCubicOdsIngestion.Repo configuration, a function
  invoked prior to each DB connection. `config` is the configured connection values
  and it returns a new set of config values to be used when connecting.
  """
  @spec before_connect(map()) :: map()
  def before_connect(config) do
    # generate a token as a password for RDS database if indicated to
    if Application.get_env(:ex_cubic_ods_ingestion, ExCubicOdsIngestion.Repo)[:use_iam_token] do
      username = Keyword.fetch!(config, :username)
      hostname = Keyword.fetch!(config, :hostname)
      port = Keyword.fetch!(config, :port)
      token = apply(ExAws.RDS, :generate_db_auth_token, [hostname, username, port, %{}])

      # update password with token
      Keyword.put(config, :password, token)
    else
      config
    end
  end
end
