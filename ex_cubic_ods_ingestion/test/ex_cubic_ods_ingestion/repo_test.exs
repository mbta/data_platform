defmodule ExCubicOdsIngestion.RepoTest do
  use ExUnit.Case

  require Logger

  defmodule MockExAws.RDS do
    @spec generate_db_auth_token(
            hostname :: String.t(),
            username :: String.t(),
            port :: integer(),
            config :: map()
          ) :: String.t()
    def generate_db_auth_token(_username, _hostname, _port, _config) do
      "iam_token"
    end
  end

  describe "before_connect/1" do
    test "uses given password if no rds module configured" do
      config =
        ExCubicOdsIngestion.Repo.before_connect(
          username: "u",
          hostname: "h",
          port: 4000,
          password: "pass"
        )

      assert Keyword.fetch!(config, :password) == "pass"
    end

    test "generates RDS IAM auth token if rds module is configured" do
      # get current repo config
      current_repo_config = Application.get_env(:ex_cubic_ods_ingestion, ExCubicOdsIngestion.Repo)
      # update manually
      updated_repo_config =
        Keyword.merge(current_repo_config, use_iam_token: true, lib_ex_aws_rds: MockExAws.RDS)

      # update the repo config
      Application.put_env(:ex_cubic_ods_ingestion, ExCubicOdsIngestion.Repo, updated_repo_config)

      connection_config =
        ExCubicOdsIngestion.Repo.before_connect(username: "u", hostname: "h", port: 4000)

      assert Keyword.fetch!(connection_config, :password) == "iam_token"
    end
  end
end
