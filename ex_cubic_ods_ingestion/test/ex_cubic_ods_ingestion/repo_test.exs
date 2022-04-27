defmodule ExCubicOdsIngestion.RepoTest do
  use ExUnit.Case

  alias ExCubicOdsIngestion.Repo

  require Logger

  # configuration of each test
  setup do
    # configure RDS module
    current_repo_config = Application.get_env(:ex_cubic_ods_ingestion, Repo)
    updated_repo_config = Keyword.merge(current_repo_config, lib_ex_aws_rds: MockExAws.RDS)
    Application.put_env(:ex_cubic_ods_ingestion, Repo, updated_repo_config)

    # put back configuration when exiting each test
    on_exit(fn ->
      Application.put_env(:ex_cubic_ods_ingestion, Repo, current_repo_config)
    end)
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

    test "generates RDS IAM configuration if rds module is configured" do
      # set use_iam_token to true
      current_repo_config = Application.get_env(:ex_cubic_ods_ingestion, Repo)
      updated_repo_config = Keyword.merge(current_repo_config, use_iam_token: true)
      Application.put_env(:ex_cubic_ods_ingestion, Repo, updated_repo_config)

      hostname = "h"

      connection_config = Repo.before_connect(username: "u", hostname: hostname, port: 4000)

      # assert token is the same as what is mocked
      assert Keyword.fetch!(connection_config, :password) == "iam_token"

      expected_ssl_opts = [
        cacertfile: Application.app_dir(:ex_cubic_ods_ingestion, ["priv", "aws-cert-bundle.pem"]),
        verify: :verify_peer,
        server_name_indication: String.to_charlist(hostname),
        verify_fun:
          {&:ssl_verify_hostname.verify_fun/3, [check_hostname: String.to_charlist(hostname)]}
      ]

      # assert the ssl options are expected
      assert Keyword.fetch!(connection_config, :ssl_opts) == expected_ssl_opts
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
