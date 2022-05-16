defmodule ExCubicIngestion.Repo.Migrations.AddRestOdsTableRecords do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_tables [
    %{
      name: "cubic_ods_qlik__edw_media_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.MEDIA_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_date_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.DATE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_alm_hardware",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_ALM_HARDWARE/"
    },
    %{
      name: "cubic_ods_qlik__edw_reason_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.REASON_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_adjustment_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.ADJUSTMENT_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_fs_chargeability_codes",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_FS_CHARGEABILITY_CODES/"
    },
    %{
      name: "cubic_ods_qlik__edw_metric_summary_by_day",
      s3_prefix: "cubic/ods_qlik/EDW.METRIC_SUMMARY_BY_DAY/"
    },
    %{
      name: "cubic_ods_qlik__edw_patron_order_line_item",
      s3_prefix: "cubic/ods_qlik/EDW.PATRON_ORDER_LINE_ITEM/"
    },
    %{
      name: "cubic_ods_qlik__edw_credit_card_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.CREDIT_CARD_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_cmdb_ci_outage",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_CMDB_CI_OUTAGE/"
    },
    %{
      name: "cubic_ods_qlik__edw_device_event",
      s3_prefix: "cubic/ods_qlik/EDW.DEVICE_EVENT/"
    },
    %{
      name: "cubic_ods_qlik__edw_sale_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.SALE_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_time_period_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.TIME_PERIOD_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_frm_bank_fee_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.FRM_BANK_FEE_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_bnft_stg_detail_stat_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.BNFT_STG_DETAIL_STAT_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_sales_channel_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.SALES_CHANNEL_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_txn_desc_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.TXN_DESC_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_cmdb_ci_vehicle",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_CMDB_CI_VEHICLE/"
    },
    %{
      name: "cubic_ods_qlik__edw_fare_product_instance",
      s3_prefix: "cubic/ods_qlik/EDW.FARE_PRODUCT_INSTANCE/"
    },
    %{
      name: "cubic_ods_qlik__edw_benefit_order_status_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.BENEFIT_ORDER_STATUS_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_fare_prod_users_list_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.FARE_PROD_USERS_LIST_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_read_txn_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.READ_TXN_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_mo_order_li_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.MO_ORDER_LI_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_sc_req_item",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_SC_REQ_ITEM/"
    },
    %{
      name: "cubic_ods_qlik__edw_device_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.DEVICE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_fare_revenue_report_schedule",
      s3_prefix: "cubic/ods_qlik/EDW.FARE_REVENUE_REPORT_SCHEDULE/"
    },
    %{
      name: "cubic_ods_qlik__edw_read_transaction",
      s3_prefix: "cubic/ods_qlik/EDW.READ_TRANSACTION/"
    },
    %{
      name: "cubic_ods_qlik__edw_fee_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.FEE_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_oa_authority_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.OA_AUTHORITY_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_txn_channel_map",
      s3_prefix: "cubic/ods_qlik/EDW.TXN_CHANNEL_MAP/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_fs_faulty_items",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_FS_FAULTY_ITEMS/"
    },
    %{
      name: "cubic_ods_qlik__edw_business_entity_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.BUSINESS_ENTITY_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_bankcard_payment_typ_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.BANKCARD_PAYMENT_TYP_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_operator_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.OPERATOR_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_benefit_status_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.BENEFIT_STATUS_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_farerev_recovery_txn",
      s3_prefix: "cubic/ods_qlik/EDW.FAREREV_RECOVERY_TXN/"
    },
    %{
      name: "cubic_ods_qlik__edw_ride_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.RIDE_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_card_status",
      s3_prefix: "cubic/ods_qlik/EDW.CARD_STATUS/"
    },
    %{
      name: "cubic_ods_qlik__edw_stop_point_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.STOP_POINT_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_core_company",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_CORE_COMPANY/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_fs_event_code",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_FS_EVENT_CODE/"
    },
    %{
      name: "cubic_ods_qlik__edw_transaction_history",
      s3_prefix: "cubic/ods_qlik/EDW.TRANSACTION_HISTORY/"
    },
    %{
      name: "cubic_ods_qlik__edw_member_benefit",
      s3_prefix: "cubic/ods_qlik/EDW.MEMBER_BENEFIT/"
    },
    %{
      name: "cubic_ods_qlik__edw_route_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.ROUTE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_abp_tap",
      s3_prefix: "cubic/ods_qlik/EDW.ABP_TAP/"
    },
    %{
      name: "cubic_ods_qlik__edw_sample",
      s3_prefix: "cubic/ods_qlik/EDW.SAMPLE/"
    },
    %{
      name: "cubic_ods_qlik__edw_customer_benefit",
      s3_prefix: "cubic/ods_qlik/EDW.CUSTOMER_BENEFIT/"
    },
    %{
      name: "cubic_ods_qlik__edw_rider_class_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.RIDER_CLASS_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_sc_request",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_SC_REQUEST/"
    },
    %{
      name: "cubic_ods_qlik__edw_card_product",
      s3_prefix: "cubic/ods_qlik/EDW.CARD_PRODUCT/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_fs_action_codes",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_FS_ACTION_CODES/"
    },
    %{
      name: "cubic_ods_qlik__edw_route_stop_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.ROUTE_STOP_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_transit_account_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.TRANSIT_ACCOUNT_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_mo_line_item_status_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.MO_LINE_ITEM_STATUS_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__cch_stage_apportionment_rules",
      s3_prefix: "cubic/ods_qlik/CCH_STAGE.APPORTIONMENT_RULES/"
    },
    %{
      name: "cubic_ods_qlik__edw_customer_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.CUSTOMER_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_service_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.SERVICE_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_alm_stockroom",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_ALM_STOCKROOM/"
    },
    %{
      name: "cubic_ods_qlik__edw_activity_code_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.ACTIVITY_CODE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_cch_gl_summary_import",
      s3_prefix: "cubic/ods_qlik/EDW.CCH_GL_SUMMARY_IMPORT/"
    },
    %{
      name: "cubic_ods_qlik__edw_employee_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.EMPLOYEE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_fs_rpir_code_rt_cause_id",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_FS_RPIR_CODE_RT_CAUSE_ID/"
    },
    %{
      name: "cubic_ods_qlik__edw_kpi_summary_by_day",
      s3_prefix: "cubic/ods_qlik/EDW.KPI_SUMMARY_BY_DAY/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_fs_fault_codes",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_FS_FAULT_CODES/"
    },
    %{
      name: "cubic_ods_qlik__edw_payment_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.PAYMENT_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_cm_payment_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.CM_PAYMENT_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_approval_status_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.APPROVAL_STATUS_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_contract_sla",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_CONTRACT_SLA/"
    },
    %{
      name: "cubic_ods_qlik__edw_bnft_stg_batch_stat_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.BNFT_STG_BATCH_STAT_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_patron_trip",
      s3_prefix: "cubic/ods_qlik/EDW.PATRON_TRIP/"
    },
    %{
      name: "cubic_ods_qlik__edw_patronage_summary",
      s3_prefix: "cubic/ods_qlik/EDW.PATRONAGE_SUMMARY/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_cmn_location",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_CMN_LOCATION/"
    },
    %{
      name: "cubic_ods_qlik__edw_metric_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.METRIC_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_event_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.EVENT_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_device_location_history",
      s3_prefix: "cubic/ods_qlik/EDW.DEVICE_LOCATION_HISTORY/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_wm_order",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_WM_ORDER/"
    },
    %{
      name: "cubic_ods_qlik__edw_patron_account_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.PATRON_ACCOUNT_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_kpi",
      s3_prefix: "cubic/ods_qlik/EDW.KPI/"
    },
    %{
      name: "cubic_ods_qlik__edw_journal_entry",
      s3_prefix: "cubic/ods_qlik/EDW.JOURNAL_ENTRY/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_sm_asset_usage",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_SM_ASSET_USAGE/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_task_sla",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_TASK_SLA/"
    },
    %{
      name: "cubic_ods_qlik__edw_kpi_detail_events_by_day",
      s3_prefix: "cubic/ods_qlik/EDW.KPI_DETAIL_EVENTS_BY_DAY/"
    },
    %{
      name: "cubic_ods_qlik__edw_mo_order_status_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.MO_ORDER_STATUS_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_trip_payment",
      s3_prefix: "cubic/ods_qlik/EDW.TRIP_PAYMENT/"
    },
    %{
      name: "cubic_ods_qlik__edw_patron_order_payment",
      s3_prefix: "cubic/ods_qlik/EDW.PATRON_ORDER_PAYMENT/"
    },
    %{
      name: "cubic_ods_qlik__edw_card_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.CARD_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_time_increment_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.TIME_INCREMENT_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_sale_transaction",
      s3_prefix: "cubic/ods_qlik/EDW.SALE_TRANSACTION/"
    },
    %{
      name: "cubic_ods_qlik__edw_b2b_order_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.B2B_ORDER_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_kpi_rule",
      s3_prefix: "cubic/ods_qlik/EDW.KPI_RULE/"
    },
    %{
      name: "cubic_ods_qlik__edw_use_transaction",
      s3_prefix: "cubic/ods_qlik/EDW.USE_TRANSACTION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_kpi_level",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_KPI_LEVEL/"
    },
    %{
      name: "cubic_ods_qlik__edw_frm_merchant_mapping",
      s3_prefix: "cubic/ods_qlik/EDW.FRM_MERCHANT_MAPPING/"
    },
    %{
      name: "cubic_ods_qlik__edw_mo_fulfill_status_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.MO_FULFILL_STATUS_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_sale_txn_payment",
      s3_prefix: "cubic/ods_qlik/EDW.SALE_TXN_PAYMENT/"
    },
    %{
      name: "cubic_ods_qlik__edw_patron_order",
      s3_prefix: "cubic/ods_qlik/EDW.PATRON_ORDER/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_wm_task",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_WM_TASK/"
    },
    %{
      name: "cubic_ods_qlik__edw_journal_entry_type_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.JOURNAL_ENTRY_TYPE_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_pal_confirmation",
      s3_prefix: "cubic/ods_qlik/EDW.PAL_CONFIRMATION/"
    },
    %{
      name: "cubic_ods_qlik__edw_payment_summary",
      s3_prefix: "cubic/ods_qlik/EDW.PAYMENT_SUMMARY/"
    },
    %{
      name: "cubic_ods_qlik__edw_frm_crdb_acq_bank_fee",
      s3_prefix: "cubic/ods_qlik/EDW.FRM_CRDB_ACQ_BANK_FEE/"
    },
    %{
      name: "cubic_ods_qlik__edw_bnft_invoice_status_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.BNFT_INVOICE_STATUS_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_cmdb_ci",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_CMDB_CI/"
    },
    %{
      name: "cubic_ods_qlik__edw_txn_status_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.TXN_STATUS_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_fare_product_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.FARE_PRODUCT_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_bnft_inv_lineitm_typ_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.BNFT_INV_LINEITM_TYP_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_be_invoice_status_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.BE_INVOICE_STATUS_DIMENSION/"
    }
  ]

  def up do
    Repo.transaction(fn ->
      Enum.each(@ods_tables, fn ods_table ->
        ods_table_rec = Repo.insert!(%CubicTable{
          name: ods_table[:name],
          s3_prefix: ods_table[:s3_prefix]
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
