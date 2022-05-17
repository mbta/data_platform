defmodule ExCubicIngestion.Schema.CubicTableTest do
  use ExCubicIngestion.DataCase, async: true

  alias ExCubicIngestion.Schema.CubicTable

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
