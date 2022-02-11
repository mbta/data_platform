defmodule ExCubicOdsIngestion.RepoTest do
  use ExUnit.Case

  require Logger

  alias ExCubicOdsIngestion.Repo

  setup do
    # configure RDS module
    current_repo_config = Application.get_env(:ex_cubic_ods_ingestion, Repo)
    updated_repo_config = Keyword.merge(current_repo_config, lib_ex_aws_rds: MockExAws.RDS)
    Application.put_env(:ex_cubic_ods_ingestion, Repo, updated_repo_config)
  end

  describe "before_connect/1" do
    test "uses given password if no rds module configured" do
      # set use_iam_token to false
      current_repo_config = Application.get_env(:ex_cubic_ods_ingestion, Repo)
      updated_repo_config = Keyword.merge(current_repo_config, use_iam_token: false)
      Application.put_env(:ex_cubic_ods_ingestion, Repo, updated_repo_config)

      connection_config =
        Repo.before_connect(
          username: "u",
          hostname: "h",
          port: 4000,
          password: "pass"
        )

      assert Keyword.fetch!(connection_config, :password) == "pass"
    end

    test "generates RDS IAM auth token if rds module is configured" do
      # set use_iam_token to true
      current_repo_config = Application.get_env(:ex_cubic_ods_ingestion, Repo)
      updated_repo_config = Keyword.merge(current_repo_config, use_iam_token: true)
      Application.put_env(:ex_cubic_ods_ingestion, Repo, updated_repo_config)

      connection_config = Repo.before_connect(username: "u", hostname: "h", port: 4000)

      assert Keyword.fetch!(connection_config, :password) == "iam_token"
    end
  end
end

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
