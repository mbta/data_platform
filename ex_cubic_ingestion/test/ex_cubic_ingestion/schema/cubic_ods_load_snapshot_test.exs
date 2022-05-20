defmodule ExCubicIngestion.Schema.CubicOdsLoadSnapshotTest do
  use ExCubicIngestion.DataCase, async: true

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable

  describe "update_snapshot/1" do
    setup do
      # insert tables
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
        snapshot_s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv"
      })

      # insert loads
      ods_load_1 =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "ready",
          s3_key: ods_snapshot_s3_key,
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197
        })

      ods_load_2 =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "ready",
          s3_key: "cubic/ods_qlik/SAMPLE/LOAD2.csv",
          s3_modified: ~U[2022-01-01 20:50:50Z],
          s3_size: 197
        })

      ods_load_3 =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "ready",
          s3_key: ods_snapshot_s3_key,
          s3_modified: ~U[2022-01-02 20:49:50Z],
          s3_size: 197
        })

      {:ok,
       %{
         ods_table: ods_table,
         ods_load_1: ods_load_1,
         ods_load_2: ods_load_2,
         ods_load_3: ods_load_3
       }}
    end

    test "passing ODS load rec that matches snapshot_s3_key", %{
      ods_table: ods_table,
      ods_load_1: ods_load_1,
      ods_load_3: ods_load_3
    } do
      # from nil to ods_load_1.s3_modified
      CubicOdsLoadSnapshot.update_snapshot(ods_load_1)

      # assert table snapshot is updated to the load's s3_modified
      assert CubicOdsTableSnapshot.get_by!(table_id: ods_table.id).snapshot ==
               ods_load_1.s3_modified

      assert CubicOdsLoadSnapshot.get_by!(load_id: ods_load_1.id).snapshot ==
               ods_load_1.s3_modified

      # from ods_load_1.s3_modified to ods_load_3.s3_modified
      CubicOdsLoadSnapshot.update_snapshot(ods_load_3)

      assert CubicOdsTableSnapshot.get_by!(table_id: ods_table.id).snapshot ==
               ods_load_3.s3_modified

      assert CubicOdsLoadSnapshot.get_by!(load_id: ods_load_3.id).snapshot ==
               ods_load_3.s3_modified
    end

    test "passing regular ODS load rec", %{
      ods_table: ods_table,
      ods_load_1: ods_load_1,
      ods_load_2: ods_load_2
    } do
      # from nil to ods_load_1.s3_modified
      CubicOdsLoadSnapshot.update_snapshot(ods_load_1)

      assert CubicOdsTableSnapshot.get_by!(table_id: ods_table.id).snapshot ==
               ods_load_1.s3_modified

      assert CubicOdsLoadSnapshot.get_by!(load_id: ods_load_1.id).snapshot ==
               ods_load_1.s3_modified

      # note: snapshot doesn't change
      CubicOdsLoadSnapshot.update_snapshot(ods_load_2)

      assert CubicOdsTableSnapshot.get_by!(table_id: ods_table.id).snapshot ==
               ods_load_1.s3_modified

      assert CubicOdsLoadSnapshot.get_by!(load_id: ods_load_2.id).snapshot ==
               ods_load_1.s3_modified
    end
  end
end
