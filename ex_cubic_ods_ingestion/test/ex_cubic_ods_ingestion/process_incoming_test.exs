defmodule ExCubicOdsIngestion.ProcessIncomingTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.ProcessIncoming
  alias ExCubicOdsIngestion.Repo

  require MockExAws
  require MockExAws.Data

  # setup server for each test
  setup do
    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)
  end

  describe "status/0" do
    test "running state" do
      server = start_supervised!({ProcessIncoming, lib_ex_aws: MockExAws})

      assert ProcessIncoming.status(server) == :running
    end
  end
end
