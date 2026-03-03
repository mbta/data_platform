defmodule ExCubicIngestion.Repo.Migrations.AddTableBatches2and3 do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_tables [
    %{
      name: "cubic_ods_qlik__edw_account_balance_by_day",
      s3_prefix: "cubic/ods_qlik/EDW.ACCOUNT_BALANCE_BY_DAY/"
    },
    %{
      name: "cubic_ods_qlik__edw_abp_transit_account_x_token",
      s3_prefix: "cubic/ods_qlik/EDW.ABP_TRANSIT_ACCOUNT_X_TOKEN/"
    },
    %{
      name: "cubic_ods_qlik__edw_sales_summary_by_day",
      s3_prefix: "cubic/ods_qlik/EDW.SALES_SUMMARY_BY_DAY/"
    },
    %{
      name: "cubic_ods_qlik__edw_patron_order_status_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.PATRON_ORDER_STATUS_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_patron_order_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.PATRON_ORDER_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_contact_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.CONTACT_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_account_transit_liability",
      s3_prefix: "cubic/ods_qlik/EDW.ACCOUNT_TRANSIT_LIABILITY/"
    },
    %{
      name: "cubic_ods_qlik__edw_frm_src_crdb_acquirer_conf",
      s3_prefix: "cubic/ods_qlik/EDW.FRM_SRC_CRDB_ACQUIRER_CONF/"
    },
    %{
      name: "cubic_ods_qlik__edw_device_current_sw_config",
      s3_prefix: "cubic/ods_qlik/EDW.DEVICE_CURRENT_SW_CONFIG/"
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
