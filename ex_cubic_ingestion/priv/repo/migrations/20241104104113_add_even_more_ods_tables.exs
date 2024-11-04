defmodule ExCubicIngestion.Repo.Migrations.AddEvenMoreOdsTables do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_tables [
    %{
      name: "cubic_ods_qlik__cch_stage_categorization_rule",
      s3_prefix: "cubic/ods_qlik/CCH_STAGE.CATEGORIZATION_RULE/"
    },
    %{
      name: "cubic_ods_qlik__cch_stage_category",
      s3_prefix: "cubic/ods_qlik/CCH_STAGE.CATEGORY/"
    },
    %{
      name: "cubic_ods_qlik__cch_stage_reprocess_action",
      s3_prefix: "cubic/ods_qlik/CCH_STAGE.REPROCESS_ACTION/"
    },
    %{
      name: "cubic_ods_qlik__cch_stage_transaction_type",
      s3_prefix: "cubic/ods_qlik/CCH_STAGE.TRANSACTION_TYPE/"
    },
    %{
      name: "cubic_ods_qlik__edw_cashbox_event_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.CASHBOX_EVENT_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_chgbk_activity_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.CHGBK_ACTIVITY_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_pass_liab_event_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.PASS_LIAB_EVENT_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_purse_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.PURSE_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_transaction_origin_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.TRANSACTION_ORIGIN_DIMENSION/"
    }
  ]

  def up do
    Repo.transaction(fn ->
      Enum.each(@ods_tables, fn ods_table ->
        ods_table_rec = Repo.insert!(%CubicTable{
          name: ods_table[:name],
          s3_prefix: ods_table[:s3_prefix],
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
