defmodule ExCubicIngestion.Schema.CubicOdsTableSnapshotTest do
  use ExCubicIngestion.DataCase, async: true

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable

  describe "get_by!/2" do
    test "getting only records that are not deleted or exist" do
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

      ods_table_snapshot =
        Repo.insert!(%CubicOdsTableSnapshot{
          table_id: ods_table.id,
          snapshot: ~U[2022-01-01 20:49:50Z],
          snapshot_s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv"
        })

      ods_table_deleted =
        Repo.insert!(%CubicTable{
          name: "cubic_ods_qlik__deleted",
          s3_prefix: "cubic/ods_qlik/DELETED/"
        })

      # inserted deleted record
      Repo.insert!(%CubicOdsTableSnapshot{
        table_id: ods_table.id,
        snapshot: ~U[2022-01-01 20:49:50Z],
        snapshot_s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv",
        deleted_at: ~U[2022-01-02 20:50:50Z]
      })

      assert_raise Ecto.NoResultsError, fn ->
        CubicOdsTableSnapshot.get_by!(table_id: dmap_table.id)
      end

      assert ods_table_snapshot == CubicOdsTableSnapshot.get_by!(table_id: ods_table.id)

      assert_raise Ecto.NoResultsError, fn ->
        CubicOdsTableSnapshot.get_by!(table_id: ods_table_deleted.id)
      end
    end
  end

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
