defmodule ExCubicOdsIngestion.ProcessIncomingTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.ProcessIncoming
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  @load_objects [
    %{
      e_tag: "\"abc123\"",
      key: "vendor/SAMPLE/LOAD1.csv",
      last_modified: "2022-02-08T20:49:50.000Z",
      owner: nil,
      size: "197",
      storage_class: "STANDARD"
    },
    %{
      e_tag: "\"def123\"",
      key: "vendor/SAMPLE/LOAD2.csv",
      last_modified: "2022-02-08T20:49:50.000Z",
      owner: nil,
      size: "123",
      storage_class: "STANDARD"
    }
  ]

  # setup server for use throughout tests
  setup do
    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)

    # start a supervisor
    server = start_supervised!(ProcessIncoming)

    %{server: server}
  end

  describe "genserver" do
    test "run", %{server: server} do
      assert ProcessIncoming.status(server) == :running
    end
  end

  describe "run" do
    test "get list of load objects" do
      assert [@load_objects, ""] ==
               ProcessIncoming.load_objects_list("cubic_ods_qlik_test/", "")
    end

    test "insert load records" do
      new_load_recs = Enum.map(@load_objects, &ProcessIncoming.insert_load(&1))

      assert [
               %{
                 status: "ready",
                 s3_key: "vendor/SAMPLE/LOAD1.csv",
                 s3_modified: ~U[2022-02-08 20:49:50Z],
                 s3_size: 197
               },
               %{
                 status: "ready",
                 s3_key: "vendor/SAMPLE/LOAD2.csv",
                 s3_modified: ~U[2022-02-08 20:49:50Z],
                 s3_size: 123
               }
             ] ==
               Enum.map(new_load_recs, fn new_load_rec ->
                 %{
                   status: new_load_rec.status,
                   s3_key: new_load_rec.s3_key,
                   s3_modified: new_load_rec.s3_modified,
                   s3_size: new_load_rec.s3_size
                 }
               end)
    end

    test "get list of load records" do
      new_load_recs = Enum.map(@load_objects, &ProcessIncoming.insert_load(&1))

      Repo.transaction(fn -> new_load_recs end)

      assert new_load_recs ==
               ProcessIncoming.load_recs_list(List.first(@load_objects))
    end

    test "filter out already loaded load objects" do
      [first_load_object | rest_load_objects] = @load_objects

      # add the first element from the load objects
      Repo.transaction(fn -> ProcessIncoming.insert_load(first_load_object) end)

      # get the records already in the database (should be just one)
      load_recs = ProcessIncoming.load_recs_list(first_load_object)

      # filter out objects already in the database
      assert rest_load_objects ==
               Enum.filter(@load_objects, &ProcessIncoming.filter_already_added(&1, load_recs))
    end
  end
end
