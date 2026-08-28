defmodule ExCubicIngestion.Repo.Migrations.AddingTables20260828 do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_tables [
    %{
      name: "cubic_ods_qlik__edw_daily_cash_balance_summary",
      s3_prefix: "cubic/ods_qlik/EDW.DAILY_CASH_BALANCE_SUMMARY/"
    },
    %{
      name: "cubic_ods_qlik__edw_daily_pos_cash_balance_summary",
      s3_prefix: "cubic/ods_qlik/EDW.DAILY_POS_CASH_BALANCE_SUMMARY/"
    },
    %{
      name: "cubic_ods_qlik__edw_fnp_parsed_manual_journal",
      s3_prefix: "cubic/ods_qlik/EDW.FNP_PARSED_MANUAL_JOURNAL/"
    },
    %{
      name: "cubic_ods_qlik__edw_component_maint_count",
      s3_prefix: "cubic/ods_qlik/EDW.COMPONENT_MAINT_COUNT/"
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
