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

  describe "load_objects_list/3" do
    test "getting objects for test prefix" do
      state = %ProcessIncoming{lib_ex_aws: MockExAws}

      assert {MockExAws.Data.load_objects(), ""} ==
               ProcessIncoming.load_objects_list("cubic_ods_qlik_test/", state)
    end

    test "getting objects for non-existing prefix" do
      state = %ProcessIncoming{lib_ex_aws: MockExAws}

      assert {[], ""} ==
               ProcessIncoming.load_objects_list("does_not_exist/", state)
    end

    # @todo test for a non-empty continuation token
  end
end
