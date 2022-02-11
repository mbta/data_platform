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
    repo_config = Application.get_env(:ex_cubic_ods_ingestion, ExCubicOdsIngestion.Repo, [])

    # generate a token as a password for RDS database if indicated to
    if Keyword.get(repo_config, :use_iam_token, false) do
      aws_rds = Keyword.get(repo_config, :lib_ex_aws_rds, ExAws.RDS)

      username = Keyword.fetch!(config, :username)
      hostname = Keyword.fetch!(config, :hostname)
      port = Keyword.fetch!(config, :port)
      token = aws_rds.generate_db_auth_token(hostname, username, port, %{})

      # update password with token
      Keyword.put(config, :password, token)
    else
      config
    end
  end
end
