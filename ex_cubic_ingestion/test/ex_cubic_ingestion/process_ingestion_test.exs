defmodule ExCubicIngestion.ProcessIngestionTest do
  use ExCubicIngestion.DataCase
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.ProcessIngestion
  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Workers.Archive
  alias ExCubicIngestion.Workers.Error

  require MockExAws.Data
  require Logger

  setup do
    # insert tables
    dmap_table =
      Repo.insert!(%CubicTable{
        name: "cubic_dmap__sample",
        s3_prefix: "cubic/dmap/sample/"
      })

    # insert loads
    dmap_load_1 =
      Repo.insert!(%CubicLoad{
        table_id: dmap_table.id,
        status: "ready_for_archiving",
        s3_key: "cubic/dmap/sample/20220101.csv",
        s3_modified: ~U[2022-01-01 20:49:50Z],
        s3_size: 197
      })

    dmap_load_2 =
      Repo.insert!(%CubicLoad{
        table_id: dmap_table.id,
        status: "ready_for_erroring",
        s3_key: "cubic/dmap/sample/20220102.csv",
        s3_modified: ~U[2022-01-02 20:49:50Z],
        s3_size: 197
      })

    {:ok,
     %{
       dmap_load_1: dmap_load_1,
       dmap_load_2: dmap_load_2
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
      dmap_load_1: dmap_load_1,
      dmap_load_2: dmap_load_2
    } do
      assert :ok == ProcessIngestion.process_loads([dmap_load_1, dmap_load_2])
    end
  end

  describe "archive/1" do
    test "archiving load after ingestion", %{
      dmap_load_1: dmap_load_1
    } do
      # insert job
      ProcessIngestion.archive(dmap_load_1)

      # make sure record is in an "archiving" status
      assert "archiving" == CubicLoad.get!(dmap_load_1.id).status

      assert_enqueued(worker: Archive, args: %{load_rec_id: dmap_load_1.id})
    end
  end

  describe "error/1" do
    test "processing error in ingestion", %{
      dmap_load_1: dmap_load_1
    } do
      # insert job
      ProcessIngestion.error(dmap_load_1)

      # make sure record is in "erroring" status
      assert "erroring" == CubicLoad.get!(dmap_load_1.id).status

      assert_enqueued(worker: Error, args: %{load_rec_id: dmap_load_1.id})
    end
  end
end
