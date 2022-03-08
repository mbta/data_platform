defmodule ExCubicOdsIngestion.StartIngestionTest do
  use ExUnit.Case
  use Oban.Testing, repo: ExCubicOdsIngestion.Repo

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Schema.CubicOdsTable
  alias ExCubicOdsIngestion.StartIngestion
  alias ExCubicOdsIngestion.Workers.Ingest

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

  describe "run/0" do
    test "updates snapshots and schedules ingestion for ready loads" do
      # insert a new table
      table =
        Repo.insert!(%CubicOdsTable{
          name: "vendor__sample",
          s3_prefix: "vendor/SAMPLE/",
          snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
        })

      # insert load records
      {:ok, new_load_recs} =
        CubicOdsLoad.insert_new_from_objects_with_table(MockExAws.Data.load_objects(), table)

      first_load_rec = List.first(new_load_recs)

      assert :ok = StartIngestion.run()

      new_table = CubicOdsTable.get!(table.id)
      assert new_table.snapshot == first_load_rec.s3_modified

      for %{id: load_rec_id} <- new_load_recs,
          load_rec = CubicOdsLoad.get!(load_rec_id) do
        assert load_rec.status == "ingesting"
        assert load_rec.snapshot == new_table.snapshot
      end

      load_rec_ids = Enum.map(new_load_recs, & &1.id)

      assert_enqueued(worker: Ingest, args: %{load_rec_ids: load_rec_ids})
    end
  end

  describe "updated_snapshot/1" do
    test "updating with a load record that matches the table snapshot" do
      # insert a new table
      new_table_rec =
        Repo.insert!(%CubicOdsTable{
          name: "vendor__sample",
          s3_prefix: "vendor/SAMPLE/",
          snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
        })

      # insert load records
      {:ok, [first_load_rec, last_load_rec]} =
        CubicOdsLoad.insert_new_from_objects_with_table(
          MockExAws.Data.load_objects(),
          new_table_rec
        )

      new_first_load_rec = StartIngestion.update_snapshot(first_load_rec)
      new_table = CubicOdsTable.get!(new_table_rec.id)

      assert new_table.snapshot == first_load_rec.s3_modified
      assert new_first_load_rec.snapshot == new_table.snapshot

      new_last_load_rec = StartIngestion.update_snapshot(last_load_rec)
      assert new_last_load_rec.snapshot == new_table.snapshot
      # table wasn't updated in the DB
      assert CubicOdsTable.get!(new_table_rec.id).snapshot == new_table.snapshot
    end

    test "crashes when preparing with list of load records that don't have a table" do
      now = DateTime.truncate(DateTime.utc_now(), :second)

      load_rec =
        Repo.insert!(%CubicOdsLoad{
          status: "ready",
          # table_id: nil,
          s3_key: "key",
          s3_modified: now,
          s3_size: 0
        })

      assert catch_error(StartIngestion.update_snapshot(load_rec))
    end
  end
end
