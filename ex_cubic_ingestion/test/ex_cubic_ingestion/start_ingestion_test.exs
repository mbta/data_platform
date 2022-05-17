defmodule ExCubicIngestion.StartIngestionTest do
  use ExCubicIngestion.DataCase
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.StartIngestion
  alias ExCubicIngestion.Workers.Ingest

  require MockExAws.Data
  require Logger

  setup do
    # insert tables
    dmap_table =
      Repo.insert!(%CubicTable{
        name: "cubic_dmap__sample",
        s3_prefix: "cubic/dmap/sample/"
      })

    ods_table =
      Repo.insert!(%CubicTable{
        name: "cubic_ods_qlik__sample",
        s3_prefix: "cubic/ods_qlik/SAMPLE/"
      })

    # insert ODS table
    ods_snapshot_s3_key = "cubic/ods_qlik/SAMPLE/LOAD1.csv"

    Repo.insert!(%CubicOdsTableSnapshot{
      table_id: ods_table.id,
      snapshot: nil,
      snapshot_s3_key: ods_snapshot_s3_key
    })

    # insert loads
    dmap_load =
      Repo.insert!(%CubicLoad{
        table_id: dmap_table.id,
        status: "ready",
        s3_key: "cubic/dmap/sample/20220101.csv",
        s3_modified: ~U[2022-01-01 20:49:50Z],
        s3_size: 197
      })

    ods_load =
      Repo.insert!(%CubicLoad{
        table_id: ods_table.id,
        status: "ready",
        s3_key: ods_snapshot_s3_key,
        s3_modified: ~U[2022-01-01 20:49:50Z],
        s3_size: 197
      })

    {:ok,
     %{
       load_rec_ids: [
         dmap_load.id,
         ods_load.id
       ]
     }}
  end

  describe "status/0" do
    test "running state" do
      server = start_supervised!(StartIngestion)

      assert StartIngestion.status(server) == :running
    end
  end

  describe "run/0" do
    test "schedules ingestion jobs for ready loads", %{
      load_rec_ids: load_rec_ids
    } do
      :ok = StartIngestion.run()

      for load_rec_id <- load_rec_ids,
          load_rec = CubicLoad.get!(load_rec_id) do
        assert load_rec.status == "ingesting"
      end

      assert_enqueued(worker: Ingest, args: %{load_rec_ids: load_rec_ids})
    end
  end
end
