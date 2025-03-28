defmodule ExCubicIngestion.Repo.Migrations.AddWc320Tables do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_tables [
    %{
      name: "cubic_ods_qlik__edw_frm_src_crdb_acquirer_chgbk",
      s3_prefix: "cubic/ods_qlik/EDW.FRM_SRC_CRDB_ACQUIRER_CHGBK/"
    },
    %{
      name: "cubic_ods_qlik__edw_travel_mode_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.TRAVEL_MODE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_abp_reprocess_log",
      s3_prefix: "cubic/ods_qlik/EDW.ABP_REPROCESS_LOG/"
    }
  ]

  def up do
    Repo.transaction(fn ->
      Enum.each(@ods_tables, fn ods_table ->
        ods_table_rec = Repo.insert!(%CubicTable{
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
        Repo.delete!(ods_table_rec)
        Repo.delete!(CubicOdsTableSnapshot.get_by!(table_id: ods_table_rec.id))
      end)
    end)
  end
end
