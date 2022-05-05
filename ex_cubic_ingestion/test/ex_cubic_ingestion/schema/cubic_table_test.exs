defmodule ExCubicIngestion.Schema.CubicTableTest do
  use ExCubicIngestion.DataCase, async: true

  alias ExCubicIngestion.Schema.CubicTable

  setup do
    table = Repo.insert!(MockExAws.Data.table())

    {:ok, %{table: table}}
  end

  describe "get/1" do
    test "adding and getting table rec", %{table: inserted_table_rec} do
      assert inserted_table_rec == CubicTable.get!(inserted_table_rec.id)
    end
  end

  describe "filter_to_existing_prefixes/1" do
    test "providing empty prefixes list" do
      assert [] == CubicTable.filter_to_existing_prefixes([])
    end

    test "limits the provided prefixes to those with an existing table", %{table: table} do
      # note: purposely leaving out incoming bucket prefix config
      prefixes = [
        "cubic/ods_qlik/SAMPLE/",
        "cubic/ods_qlik/SAMPLE__ct/",
        "cubic/ods_qlik/SAMPLE_TABLE_WRONG/",
        "other"
      ]

      expected = [
        {"cubic/ods_qlik/SAMPLE/", table},
        {"cubic/ods_qlik/SAMPLE__ct/", table}
      ]

      actual = CubicTable.filter_to_existing_prefixes(prefixes)

      assert expected == actual
    end
  end
end
