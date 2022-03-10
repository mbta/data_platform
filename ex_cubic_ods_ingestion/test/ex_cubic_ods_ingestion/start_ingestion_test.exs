defmodule ExCubicOdsIngestion.StartIngestionTest do
  use ExCubicOdsIngestion.DataCase
  use Oban.Testing, repo: ExCubicOdsIngestion.Repo

  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Schema.CubicOdsTable
  alias ExCubicOdsIngestion.StartIngestion
  alias ExCubicOdsIngestion.Workers.Ingest

  require MockExAws.Data
  require Logger

  # setup server for each test
  setup do
    # insert table
    table = Repo.insert!(MockExAws.Data.table())

    # insert load records
    {:ok, load_recs} =
      CubicOdsLoad.insert_new_from_objects_with_table(MockExAws.Data.load_objects(), table)

    [first_load_rec, last_load_rec] = load_recs

    {:ok,
     %{
       table: table,
       load_recs: load_recs,
       first_load_rec: first_load_rec,
       last_load_rec: last_load_rec
     }}
  end

  describe "status/0" do
    test "running state" do
      server = start_supervised!(StartIngestion)

      assert StartIngestion.status(server) == :running
    end
  end

  describe "run/0" do
    test "updates snapshots and schedules ingestion for ready loads", %{
      table: table,
      first_load_rec: first_load_rec,
      load_recs: new_load_recs
    } do
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
    test "updating with a load record that matches the table snapshot", %{
      table: new_table_rec,
      first_load_rec: first_load_rec,
      last_load_rec: last_load_rec
    } do
      new_first_load_rec = StartIngestion.update_snapshot(first_load_rec)
      new_table = CubicOdsTable.get!(new_table_rec.id)

      assert new_table.snapshot == first_load_rec.s3_modified
      assert new_first_load_rec.snapshot == new_table.snapshot

      new_last_load_rec = StartIngestion.update_snapshot(last_load_rec)
      assert new_last_load_rec.snapshot == new_table.snapshot
      # table wasn't updated in the DB
      assert CubicOdsTable.get!(new_table_rec.id).snapshot == new_table.snapshot
    end
  end
end
