defmodule ExCubicIngestion.Repo.Migrations.AddMoreTablesForIngestion_20251114 do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_tables [
    %{
      name: "cubic_ods_qlik__edw_unsettled_cca_cash_count",
      s3_prefix: "cubic/ods_qlik/EDW.UNSETTLED_CCA_CASH_COUNT/"
    },
    %{
      name: "cubic_ods_qlik__edw_unsettled_crdb_acq_conf",
      s3_prefix: "cubic/ods_qlik/EDW.UNSETTLED_CRDB_ACQ_CONF/"
    },
    %{
      name: "cubic_ods_qlik__edw_unsettled_crdb_chgbk",
      s3_prefix: "cubic/ods_qlik/EDW.UNSETTLED_CRDB_CHGBK/"
    },
    %{
      name: "cubic_ods_qlik__edw_unsettled_device_cash_stc",
      s3_prefix: "cubic/ods_qlik/EDW.UNSETTLED_DEVICE_CASH_STC/"
    },
    %{
      name: "cubic_ods_qlik__edw_unsettled_dist_order",
      s3_prefix: "cubic/ods_qlik/EDW.UNSETTLED_DIST_ORDER/"
    },
    %{
      name: "cubic_ods_qlik__edw_unsettled_misc",
      s3_prefix: "cubic/ods_qlik/EDW.UNSETTLED_MISC/"
    },
    %{
      name: "cubic_ods_qlik__edw_unsettled_patron_order",
      s3_prefix: "cubic/ods_qlik/EDW.UNSETTLED_PATRON_ORDER/"
    },
    %{
      name: "cubic_ods_qlik__edw_unsettled_sale",
      s3_prefix: "cubic/ods_qlik/EDW.UNSETTLED_SALE/"
    },
    %{
      name: "cubic_ods_qlik__edw_unsettled_use",
      s3_prefix: "cubic/ods_qlik/EDW.UNSETTLED_USE/"
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
        Repo.delete!(ods_table_rec)
        Repo.delete!(CubicOdsTableSnapshot.get_by!(table_id: ods_table_rec.id))
      end)
    end)
  end
end
