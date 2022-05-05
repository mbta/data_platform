defmodule ExCubicIngestion.Schema.ObanIngestWorkerErrorTest do
  @moduledoc """
  Test for ingest worker error handler.
  """

  use ExCubicIngestion.DataCase, async: true

  alias ExCubicIngestion.ObanIngestWorkerError
  alias ExCubicIngestion.Schema.CubicLoad

  setup do
    table = Repo.insert!(MockExAws.Data.table())
    {:ok, %{table: table, load_objects: MockExAws.Data.load_objects_without_bucket_prefix()}}
  end

  describe "handle_event/4" do
    test "taking action on attemps as it reaches max attempts", %{
      table: table,
      load_objects: load_objects
    } do
      {:ok, new_load_recs} = CubicLoad.insert_new_from_objects_with_table(load_objects, table)

      new_load_rec_ids = Enum.map(new_load_recs, fn rec -> rec.id end)

      worker_meta_data = %{
        worker: "ExCubicIngestion.Workers.Ingest",
        attempt: 1,
        max_attempts: 2,
        args: %{
          "load_rec_ids" => new_load_rec_ids
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

      # increment attempts
      updated_load_recs =
        ObanIngestWorkerError.handle_event(
          [:oban, :job, :exception],
          nil,
          %{worker_meta_data | attempt: 2},
          nil
        )

      # status should be updated for records
      assert ["ready_for_erroring", "ready_for_erroring"] ==
               Enum.map(updated_load_recs, fn rec -> rec.status end)
    end
  end
end
