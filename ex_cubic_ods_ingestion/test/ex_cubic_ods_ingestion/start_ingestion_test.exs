defmodule ExCubicOdsIngestion.StartIngestionTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.StartIngestion
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Schema.CubicOdsTable

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
      server = start_supervised!({StartIngestion, lib_ex_aws: MockExAws})

      assert StartIngestion.status(server) == :running
    end
  end

  describe "attach_table/1" do
    test "attaching with load records" do
      # insert a new table
      new_table_rec = %CubicOdsTable{
        name: "vendor__sample",
        s3_prefix: "vendor/SAMPLE/",
        snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
      }

      {:ok, new_table_rec} =
        Repo.transaction(fn ->
          Repo.insert!(new_table_rec)
        end)

      # insert load records
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)
      last_load_rec = List.last(new_load_recs)

      # attach table
      {first_load_rec, first_load_table_rec} = StartIngestion.attach_table(first_load_rec)
      {last_load_rec, _last_load_table_rec} = StartIngestion.attach_table(last_load_rec)

      # assert that we attached the right table, and we have the right updates
      assert %{
               first_load_table_id: new_table_rec.id,
               first_load_snapshot: first_load_rec.s3_modified,
               last_load_table_id: new_table_rec.id,
               last_load_snapshot: last_load_rec.s3_modified,
               table_id: new_table_rec.id,
               table_snapshot: first_load_rec.s3_modified
             } == %{
               first_load_table_id: first_load_rec.table_id,
               first_load_snapshot: first_load_rec.snapshot,
               last_load_table_id: last_load_rec.table_id,
               last_load_snapshot: last_load_rec.s3_modified,
               table_id: first_load_table_rec.id,
               table_snapshot: first_load_table_rec.snapshot
             }
    end

    test "not attaching anything by providing a load with no known table association" do
      # insert load records
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)

      assert {first_load_rec, nil} == StartIngestion.attach_table(first_load_rec)
    end
  end

  describe "start_ingestion/1" do
    test "kicking off worker" do
      # insert a new table
      new_table_rec = %CubicOdsTable{
        name: "vendor__sample",
        s3_prefix: "vendor/SAMPLE/",
        snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
      }

      {:ok, _new_table_rec} =
        Repo.transaction(fn ->
          Repo.insert!(new_table_rec)
        end)

      # insert load records
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)

      # attach the table and try to ingest
      StartIngestion.attach_table(first_load_rec) |> StartIngestion.start_ingestion()

      assert "ingesting" == CubicOdsLoad.get(first_load_rec.id).status
    end

    test "erroring out because of no table association" do
      # insert load records
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)

      # try to ingest
      StartIngestion.start_ingestion({first_load_rec, nil})

      assert "errored" == CubicOdsLoad.get(first_load_rec.id).status
    end
  end
end
