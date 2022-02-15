defmodule ExCubicOdsIngestion.ProcessIncomingTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.ProcessIncoming
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  # setup server for use throughout tests
  setup do
    Application.put_env(:ex_cubic_ods_ingestion, :lib_ex_aws, MockExAws)

    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)

    # start a supervisor
    server = start_supervised!(ProcessIncoming)

    %{server: server}
  end

  describe "status/0" do
    test "running state", %{server: server} do
      assert ProcessIncoming.status(server) == :running
    end
  end

  describe "load_objects_list/3" do
    test "getting objects for test prefix" do
      assert [MockExAws.Data.load_objects(), ""] ==
           ProcessIncoming.load_objects_list("cubic_ods_qlik_test/", "")
    end

    test "getting objects for non-existing prefix" do
      assert [[], ""] ==
           ProcessIncoming.load_objects_list("does_not_exist/", "")
    end

    # @todo test for a non-empty continuation token
  end

  describe "load_recs_list/1" do
    test "getting records just added by providing the first load object in list" do
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())

      assert new_load_recs == ProcessIncoming.load_recs_list(List.first(MockExAws.Data.load_objects()))
    end

    test "getting the last record by providing the last load object in list" do
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())

      assert [List.last(new_load_recs)] == ProcessIncoming.load_recs_list(List.last(MockExAws.Data.load_objects()))
    end

    test "getting no records by providing a load object not in db" do
      {:ok, _new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())

      assert [] == ProcessIncoming.load_recs_list(%{
        e_tag: "\"ghi789\"",
        key: "not/in/db.csv",
        last_modified: "2022-02-08T21:49:50.000Z",
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      })
    end

    test "getting no records by NOT providing load object" do
      assert [] == ProcessIncoming.load_recs_list(nil)
    end

    # @todo test for improper load object map
  end

  describe "not_added/2" do
    test "object NOT found in database records" do
      load_object = List.first(MockExAws.Data.load_objects())
      load_recs = [%CubicOdsLoad{
        s3_key: "key/not/found.csv",
        s3_modified: ~U[2022-02-08 20:49:50Z]
      }]

      assert ProcessIncoming.not_added(load_object, load_recs)
    end

    test "object found in database records" do
      load_object = List.first(MockExAws.Data.load_objects())
      load_recs = [%CubicOdsLoad{
        s3_key: "vendor/SAMPLE/LOAD1.csv",
        s3_modified: ~U[2022-02-08 20:49:50Z]
      }]

      assert not ProcessIncoming.not_added(load_object, load_recs)
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
