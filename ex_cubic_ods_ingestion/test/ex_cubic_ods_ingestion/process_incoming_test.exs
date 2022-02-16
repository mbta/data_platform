defmodule ExCubicOdsIngestion.ProcessIncomingTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.ProcessIncoming
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  require MockExAws.Data
  require Logger

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

  describe "not_added/2" do
    test "object NOT found in database records" do
      load_object = List.first(MockExAws.Data.load_objects())

      load_recs = [
        %CubicOdsLoad{
          s3_key: "key/not/found.csv",
          s3_modified: ~U[2022-02-08 20:49:50Z]
        }
      ]

      assert ProcessIncoming.not_added(load_object, load_recs)
    end

    test "object found in database records" do
      load_object = List.first(MockExAws.Data.load_objects())

      load_recs = [
        %CubicOdsLoad{
          s3_key: "vendor/SAMPLE/LOAD1.csv",
          s3_modified: ~U[2022-02-08 20:49:50Z]
        }
      ]

      refute ProcessIncoming.not_added(load_object, load_recs)
    end
  end
end

defmodule MockExAws do
  @moduledoc """
  MockExAws @todo
  """

  @spec request!(ExAws.Operation.t(), keyword) :: term
  def request!(op, _config_overrides \\ []) do
    incoming_prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_prefix_incoming)

    if op.params["prefix"] == "#{incoming_prefix}cubic_ods_qlik_test/" do
      %{
        body: %{
          contents: MockExAws.Data.load_objects(),
          next_continuation_token: ""
        }
      }
    else
      %{
        body: %{
          contents: [],
          next_continuation_token: ""
        }
      }
    end

    # ::::: original implementation :::::
    #
    # case request(op, config_overrides) do
    #   {:ok, result} ->
    #     result

    #   error ->
    #     raise ExAws.Error, """
    #     ExAws Request Error!
    #     #{inspect(error)}
    #     """
    # end
  end
end
