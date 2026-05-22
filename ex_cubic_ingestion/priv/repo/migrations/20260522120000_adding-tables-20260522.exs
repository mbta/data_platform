defmodule ExCubicIngestion.Repo.Migrations.AddingTables20260522 do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_tables [
    %{
      name: "cubic_ods_qlik__edw_cch_reprocess_action",
      s3_prefix: "cubic/ods_qlik/EDW.CCH_REPROCESS_ACTION/"
    },
    %{
      name: "cubic_ods_qlik__edw_gl_sum_rec_stat_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.GL_SUM_REC_STAT_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_cch_account",
      s3_prefix: "cubic/ods_qlik/EDW.CCH_ACCOUNT/"
    },
    %{
      name: "cubic_ods_qlik__edw_cch_apportion_rule_map",
      s3_prefix: "cubic/ods_qlik/EDW.CCH_APPORTION_RULE_MAP/"
    },
    %{
      name: "cubic_ods_qlik__edw_cch_category",
      s3_prefix: "cubic/ods_qlik/EDW.CCH_CATEGORY/"
    },
  ]

  def up do
    Repo.transaction(fn ->
      Enum.each(@ods_tables, fn ods_table ->
        ods_table_rec =
          Repo.insert!(%CubicTable{
            name: ods_table[:name],
            s3_prefix: ods_table[:s3_prefix],
            is_active: true,
            is_raw: true
          })

        Repo.insert!(%CubicOdsTableSnapshot{
          table_id: ods_table_rec.id,
          snapshot_s3_key: "#{ods_table[:s3_prefix]}LOAD00000001.csv.gz"
        })
      end)
    end)
  end

  def down do
    Repo.transaction(fn ->
      Enum.each(@ods_tables, fn ods_table ->
        ods_table_rec = CubicTable.get_by!(name: ods_table[:name])
        Repo.delete!(CubicOdsTableSnapshot.get_by!(table_id: ods_table_rec.id))
        Repo.delete!(ods_table_rec)
      end)
    end)
  end
end
