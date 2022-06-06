defmodule ExCubicIngestion.Schema.CubicTableTest do
  use ExCubicIngestion.DataCase, async: true

  alias ExCubicIngestion.Schema.CubicTable

  describe "get_by!/2" do
    test "getting only items that are not deleted or exit" do
      dmap_table =
        Repo.insert!(%CubicTable{
          name: "cubic_dmap__sample",
          s3_prefix: "cubic/dmap/sample/"
        })

      # insert deleted record
      Repo.insert!(%CubicTable{
        name: "cubic_ods_qlik__sample",
        s3_prefix: "cubic/ods_qlik/SAMPLE/",
        deleted_at: ~U[2022-01-01 20:50:50Z]
      })

      assert dmap_table == CubicTable.get_by!(name: "cubic_dmap__sample")

      assert_raise Ecto.NoResultsError, fn ->
        CubicTable.get_by!(name: "cubic_ods_qlik__sample")
      end

      assert_raise Ecto.NoResultsError, fn ->
        CubicTable.get_by!(name: "cubic_ods_qlik__does_not_exist")
      end
    end
  end

  describe "filter_to_existing_prefixes/1" do
    test "providing empty prefixes list" do
      assert [] == CubicTable.filter_to_existing_prefixes([])
    end

    test "limits the provided prefixes to those with an existing table" do
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

      # note: purposely leaving out incoming bucket prefix config
      prefixes = [
        "cubic/dmap/sample/",
        "cubic/dmap/sample_table_wrong/",
        "cubic/ods_qlik/SAMPLE/",
        "cubic/ods_qlik/SAMPLE__ct/",
        "cubic/ods_qlik/SAMPLE_TABLE_WRONG/",
        "other"
      ]

      expected = [
        {"cubic/dmap/sample/", dmap_table},
        {"cubic/ods_qlik/SAMPLE/", ods_table},
        {"cubic/ods_qlik/SAMPLE__ct/", ods_table}
      ]

      actual = CubicTable.filter_to_existing_prefixes(prefixes)

      assert expected == actual
    end
  end
end
