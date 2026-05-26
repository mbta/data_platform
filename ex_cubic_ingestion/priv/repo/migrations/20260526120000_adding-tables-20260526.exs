defmodule ExCubicIngestion.Repo.Migrations.AddingTables20260526 do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_tables [
    %{
      name: "cubic_ods_qlik__edw_card_action",
      s3_prefix: "cubic/ods_qlik/EDW.CARD_ACTION/"
    },
    %{
      name: "cubic_ods_qlik__edw_card_action_reason_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.CARD_ACTION_REASON_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_card_action_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.CARD_ACTION_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_fraud_alert_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.FRAUD_ALERT_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_fraud_summary_by_account",
      s3_prefix: "cubic/ods_qlik/EDW.FRAUD_SUMMARY_BY_ACCOUNT/"
    },
    %{
      name: "cubic_ods_qlik__edw_fraud_summary_by_day",
      s3_prefix: "cubic/ods_qlik/EDW.FRAUD_SUMMARY_BY_DAY/"
    },
    %{
      name: "cubic_ods_qlik__edw_ifdm_fraud_alert",
      s3_prefix: "cubic/ods_qlik/EDW.IFDM_FRAUD_ALERT/"
    },
    %{
      name: "cubic_ods_qlik__edw_ifdm_fraud_alert_action",
      s3_prefix: "cubic/ods_qlik/EDW.IFDM_FRAUD_ALERT_ACTION/"
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
