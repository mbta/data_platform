defmodule ExCubicOdsIngestion.ProcessIngestionTest do
  use ExUnit.Case
  use Oban.Testing, repo: ExCubicOdsIngestion.Repo

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.ProcessIngestion
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Workers.Archive
  alias ExCubicOdsIngestion.Workers.Error

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
      server = start_supervised!(ProcessIngestion)

      assert ProcessIngestion.status(server) == :running
    end
  end

  describe "process_loads/1" do
    test "processing empty list" do
      assert :ok == ProcessIngestion.process_loads([])
    end

    test "processing with one ready for archiving load and one ready for erroring" do
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)
      last_load_rec = List.last(new_load_recs)

      CubicOdsLoad.update(first_load_rec, %{status: "ready_for_archiving"})
      CubicOdsLoad.update(last_load_rec, %{status: "ready_for_erroring"})

      assert :ok == ProcessIngestion.process_loads(new_load_recs)
    end
  end

  describe "archive/1" do
    test "archiving load after ingestion" do
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)

      # insert job
      ProcessIngestion.archive(first_load_rec)

      # make sure at least of the load records is in an "archiving" status
      assert "archiving" == CubicOdsLoad.get!(first_load_rec.id).status

      assert_enqueued(worker: Archive, args: %{load_rec_id: first_load_rec.id})
    end
  end

  describe "error/1" do
    test "processing error in ingestion" do
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)

      # insert job
      ProcessIngestion.error(first_load_rec)

      # make sure at least of the load records is in an "archiving" status
      assert "erroring" == CubicOdsLoad.get!(first_load_rec.id).status

      assert_enqueued(worker: Error, args: %{load_rec_id: first_load_rec.id})
    end
  end
end
