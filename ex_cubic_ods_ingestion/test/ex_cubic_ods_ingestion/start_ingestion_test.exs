defmodule ExCubicOdsIngestion.StartIngestionTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Schema.CubicOdsTable
  alias ExCubicOdsIngestion.StartIngestion

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
      server = start_supervised!(StartIngestion)

      assert StartIngestion.status(server) == :running
    end
  end

  describe "prepare_loads/1" do
    test "preparing with an empty list" do
      assert {[], []} == StartIngestion.prepare_loads([])
    end

    test "preparing with list of load records that have a table" do
      # insert a new table
      new_table_rec = %CubicOdsTable{
        name: "vendor__sample",
        s3_prefix: "vendor/SAMPLE/",
        snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
      }

      {:ok, _inserted_table_rec} =
        Repo.transaction(fn ->
          Repo.insert!(new_table_rec)
        end)

      # insert load records
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)
      last_load_rec = List.last(new_load_recs)

      # attach tables
      {attached_first_load_rec, attached_first_load_table_rec} =
        StartIngestion.attach_table(first_load_rec)

      {attached_last_load_rec, attached_last_load_table_rec} =
        StartIngestion.attach_table(last_load_rec)

      # prepare loads and get first chunk
      {prepared_error_loads, prepared_ready_loads_chunks} =
        StartIngestion.prepare_loads(new_load_recs)

      first_prepared_ready_loads_chunk = List.first(prepared_ready_loads_chunks)

      # assert error loads is empty
      assert [] == prepared_error_loads

      # assert the first chunk matches load recs and table inserted
      assert [
               %{
                 load_table_id: attached_first_load_rec.table_id,
                 load_snapshot: attached_first_load_rec.snapshot,
                 table_id: attached_first_load_table_rec.id,
                 table_snapshot: attached_first_load_table_rec.snapshot
               },
               %{
                 load_table_id: attached_last_load_rec.table_id,
                 load_snapshot: attached_last_load_rec.snapshot,
                 table_id: attached_last_load_table_rec.id,
                 table_snapshot: attached_last_load_table_rec.snapshot
               }
             ] ==
               Enum.map(first_prepared_ready_loads_chunk, fn {chunk_load_rec, chunk_table_rec} ->
                 %{
                   load_table_id: chunk_load_rec.table_id,
                   load_snapshot: chunk_load_rec.snapshot,
                   table_id: chunk_table_rec.id,
                   table_snapshot: chunk_table_rec.snapshot
                 }
               end)
    end

    test "preparing with list of load records that don't have a table" do
      # insert load records
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())

      assert {Enum.map(new_load_recs, fn new_load_rec -> {new_load_rec, nil} end), []} ==
               StartIngestion.prepare_loads(new_load_recs)
    end
  end

  describe "process_loads/1" do
    test "processing with empty values" do
      assert :ok == StartIngestion.process_loads({[], []})
    end

    test "processing with just empty ready load chunks list" do
      # insert load records
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)

      assert :ok ==
               StartIngestion.process_loads({
                 [{first_load_rec, nil}],
                 []
               })
    end

    test "processing with just empty error load list" do
      # insert a new table
      new_table_rec = %CubicOdsTable{
        name: "vendor__sample",
        s3_prefix: "vendor/SAMPLE/",
        snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
      }

      {:ok, _inserted_table_rec} =
        Repo.transaction(fn ->
          Repo.insert!(new_table_rec)
        end)

      # insert load records
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)

      # attach table
      {attached_first_load_rec, attached_first_load_table_rec} =
        StartIngestion.attach_table(first_load_rec)

      assert :ok ==
               StartIngestion.process_loads({
                 [],
                 [
                   [{attached_first_load_rec, attached_first_load_table_rec}]
                 ]
               })
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

      {:ok, inserted_table_rec} =
        Repo.transaction(fn ->
          Repo.insert!(new_table_rec)
        end)

      # insert load records
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)

      # attach table
      {attached_first_load_rec, attached_first_load_table_rec} =
        StartIngestion.attach_table(first_load_rec)

      # assert that we attached the right table, and we have the right updates
      assert %{
               first_load_table_id: inserted_table_rec.id,
               first_load_snapshot: first_load_rec.s3_modified,
               table_id: inserted_table_rec.id,
               table_snapshot: first_load_rec.s3_modified
             } == %{
               first_load_table_id: attached_first_load_rec.table_id,
               first_load_snapshot: attached_first_load_rec.snapshot,
               table_id: attached_first_load_table_rec.id,
               table_snapshot: attached_first_load_table_rec.snapshot
             }

      # re-attach to simulate a re-run
      {reattached_first_load_rec, reattached_first_load_table_rec} =
        StartIngestion.attach_table(attached_first_load_rec)

      assert %{
               first_load_table_id: inserted_table_rec.id,
               first_load_snapshot: first_load_rec.s3_modified,
               table_id: inserted_table_rec.id,
               table_snapshot: first_load_rec.s3_modified
             } == %{
               first_load_table_id: reattached_first_load_rec.table_id,
               first_load_snapshot: reattached_first_load_rec.snapshot,
               table_id: reattached_first_load_table_rec.id,
               table_snapshot: reattached_first_load_table_rec.snapshot
             }
    end

    test "not attaching anything by providing a load with no known table association" do
      # insert load records
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())
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

      {:ok, _inserted_table_rec} =
        Repo.transaction(fn ->
          Repo.insert!(new_table_rec)
        end)

      # insert load records
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)
      last_load_rec = List.last(new_load_recs)

      # attach tables
      StartIngestion.attach_table(first_load_rec)
      StartIngestion.attach_table(last_load_rec)

      # prepare loads and get first chunk
      {_prepared_error_loads, prepared_ready_loads_chunks} =
        StartIngestion.prepare_loads(new_load_recs)

      first_prepared_ready_loads_chunk = List.first(prepared_ready_loads_chunks)

      # attach the table and create oban job
      {:ok, oban_job} =
        StartIngestion.start_ingestion(
          Enum.map(first_prepared_ready_loads_chunk, fn {load_rec, _table_rec} -> load_rec.id end)
        )

      assert "available" == oban_job.state
    end
  end
end
