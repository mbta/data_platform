defmodule ExCubicIngestion.Repo.Migrations.AddingTables20260330 do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_tables [
    %{
      name: "cubic_ods_qlik__edw_proc_cost_pg_loss_dtl",
      s3_prefix: "cubic/ods_qlik/EDW.PROC_COST_PG_LOSS_DTL/"
    },
    %{
      name: "cubic_ods_qlik__edw_proc_cost_chgbk_dtl",
      s3_prefix: "cubic/ods_qlik/EDW.PROC_COST_CHGBK_DTL/"
    },
    %{
      name: "cubic_ods_qlik__edw_process_cost_summary",
      s3_prefix: "cubic/ods_qlik/EDW.PROCESS_COST_SUMMARY/"
    },
    %{
      name: "cubic_ods_qlik__edw_frm_crdb_acq_bank_fee_detail",
      s3_prefix: "cubic/ods_qlik/EDW.FRM_CRDB_ACQ_BANK_FEE_DETAIL/"
    },
    %{
      name: "cubic_ods_qlik__edw_frm_bank_fee_summary",
      s3_prefix: "cubic/ods_qlik/EDW.FRM_BANK_FEE_SUMMARY/"
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
