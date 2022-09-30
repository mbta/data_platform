defmodule ExCubicIngestion.SchemaFetchTest do
  use ExCubicIngestion.DataCase, async: true

  import ExCubicIngestion.TestFixtures, only: [setup_tables_loads: 1]

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.SchemaFetch

  setup :setup_tables_loads

  describe "get_cubic_ods_qlik_columns/2" do
    test "getting cubic provided schema file (dfm) for batch load", %{
      ods_load: ods_load
    } do
      assert [
               "sample_id",
               "sample_name",
               "edw_inserted_dtm",
               "edw_updated_dtm"
             ] == SchemaFetch.get_cubic_ods_qlik_columns(MockExAws, ods_load)
    end

    test "getting cubic provided schema file (dfm) for change tracking load", %{
      ods_table: ods_table
    } do
      ods_ct_load =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "ready",
          s3_key: "cubic/ods_qlik/SAMPLE__ct/20220102-204950123.csv.gz",
          s3_modified: ~U[2022-01-02 20:49:50Z],
          s3_size: 197
        })

      assert [
               "header__change_seq",
               "header__change_oper",
               "header__timestamp",
               "sample_id",
               "sample_name",
               "edw_inserted_dtm",
               "edw_updated_dtm"
             ] == SchemaFetch.get_cubic_ods_qlik_columns(MockExAws, ods_ct_load)
    end
  end

  describe "get_glue_columns/2" do
    test "getting glue table columns for ODS/batch loads and ODS change tracking loads", %{
      ods_table: ods_table,
      ods_load: ods_load
    } do
      assert [
               "sample_id",
               "sample_name",
               "edw_inserted_dtm",
               "edw_updated_dtm"
             ] == SchemaFetch.get_glue_columns(MockExAws, ods_table, ods_load)

      ods_ct_load =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "ready",
          s3_key: "cubic/ods_qlik/SAMPLE__ct/20220102-204950123.csv.gz",
          s3_modified: ~U[2022-01-02 20:49:50Z],
          s3_size: 197
        })

      assert [
               "header__change_seq",
               "header__change_oper",
               "header__timestamp",
               "sample_id",
               "sample_name",
               "edw_inserted_dtm",
               "edw_updated_dtm"
             ] == SchemaFetch.get_glue_columns(MockExAws, ods_table, ods_ct_load)
    end
  end
end
