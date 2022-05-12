defmodule ExCubicIngestion.Schema.ObanIngestWorkerErrorTest do
  @moduledoc """
  Test for ingest worker error handler.
  """

  use ExCubicIngestion.DataCase, async: true

  alias ExCubicIngestion.ObanIngestWorkerError
  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicTable

  describe "handle_event/4" do
    test "taking action on attemps as it reaches max attempts" do
      dmap_table =
        Repo.insert!(%CubicTable{
          name: "cubic_dmap__sample",
          s3_prefix: "cubic/dmap/sample/"
        })

      dmap_load_1 =
        Repo.insert!(%CubicLoad{
          table_id: dmap_table.id,
          status: "ingesting",
          s3_key: "cubic/dmap/sample/20220101.csv",
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197
        })

      dmap_load_2 =
        Repo.insert!(%CubicLoad{
          table_id: dmap_table.id,
          status: "ready",
          s3_key: "cubic/dmap/sample/20220102.csv",
          s3_modified: ~U[2022-01-02 20:49:50Z],
          s3_size: 197
        })

      worker_meta_data = %{
        worker: "ExCubicIngestion.Workers.Ingest",
        attempt: 1,
        max_attempts: 2,
        args: %{
          "load_rec_ids" => [dmap_load_1.id, dmap_load_2.id]
        }
      }

      # no updates should have been made
      assert [] ==
               ObanIngestWorkerError.handle_event(
                 [:oban, :job, :exception],
                 nil,
                 worker_meta_data,
                 nil
               )

      updated_load_recs =
        ObanIngestWorkerError.handle_event(
          [:oban, :job, :exception],
          nil,
          # increment attempts
          %{worker_meta_data | attempt: 2},
          nil
        )

      # status should be updated for records
      assert ["ready_for_erroring", "ready_for_erroring"] ==
               Enum.map(updated_load_recs, & &1.status)
    end
  end
end
