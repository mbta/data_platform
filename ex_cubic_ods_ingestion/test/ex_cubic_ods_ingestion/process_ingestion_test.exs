defmodule ExCubicOdsIngestion.ProcessIngestionTest do
  use ExCubicOdsIngestion.DataCase
  use Oban.Testing, repo: ExCubicOdsIngestion.Repo

  alias ExCubicOdsIngestion.ProcessIngestion
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Workers.Archive
  alias ExCubicOdsIngestion.Workers.Error

  require MockExAws.Data
  require Logger

  setup do
    # insert table
    table = Repo.insert!(MockExAws.Data.table())

    # insert load records
    {:ok, load_recs} =
      CubicOdsLoad.insert_new_from_objects_with_table(
        MockExAws.Data.load_objects_without_bucket_prefix(),
        table
      )

    [first_load_rec, last_load_rec] = load_recs

    {:ok,
     %{
       load_recs: load_recs,
       first_load_rec: first_load_rec,
       last_load_rec: last_load_rec
     }}
  end

  describe "status/0" do
    test "running state" do
      server = start_supervised!(ProcessIngestion)

      assert ProcessIngestion.status(server) == :running
    end
  end

  describe "process_loads/1" do
    test "processing empty list" do
      assert :ok == ProcessIngestion.process_loads([])
    end

    test "processing with one ready for archiving load and one ready for erroring", %{
      load_recs: new_load_recs,
      first_load_rec: first_load_rec,
      last_load_rec: last_load_rec
    } do
      CubicOdsLoad.update(first_load_rec, %{status: "ready_for_archiving"})
      CubicOdsLoad.update(last_load_rec, %{status: "ready_for_erroring"})

      assert :ok == ProcessIngestion.process_loads(new_load_recs)
    end
  end

  describe "archive/1" do
    test "archiving load after ingestion", %{
      first_load_rec: first_load_rec
    } do
      # insert job
      ProcessIngestion.archive(first_load_rec)

      # make sure at least of the load records is in an "archiving" status
      assert "archiving" == CubicOdsLoad.get!(first_load_rec.id).status

      assert_enqueued(worker: Archive, args: %{load_rec_id: first_load_rec.id})
    end
  end

  describe "error/1" do
    test "processing error in ingestion", %{
      first_load_rec: first_load_rec
    } do
      # insert job
      ProcessIngestion.error(first_load_rec)

      # make sure at least of the load records is in an "archiving" status
      assert "erroring" == CubicOdsLoad.get!(first_load_rec.id).status

      assert_enqueued(worker: Error, args: %{load_rec_id: first_load_rec.id})
    end
  end
end
