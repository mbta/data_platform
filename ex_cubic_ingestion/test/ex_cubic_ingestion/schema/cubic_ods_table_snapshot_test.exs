defmodule ExCubicIngestion.Schema.CubicOdsTableSnapshotTest do
  use ExCubicIngestion.DataCase, async: true

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable

  describe "update_snapshot/2" do
    test "update the snapshot value of a table record" do
      ods_table =
        Repo.insert!(%CubicTable{
          name: "cubic_ods_qlik__sample",
          s3_prefix: "cubic/ods_qlik/SAMPLE/"
        })

      # insert ODS table
      ods_table_snapshot =
        Repo.insert!(%CubicOdsTableSnapshot{
          table_id: ods_table.id,
          snapshot: ~U[2022-01-01 20:49:50Z],
          snapshot_s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv"
        })

      updated_snapshot = ~U[2022-01-02 20:49:50Z]

      assert updated_snapshot ==
               CubicOdsTableSnapshot.update_snapshot(ods_table_snapshot, updated_snapshot).snapshot
    end
  end
end
