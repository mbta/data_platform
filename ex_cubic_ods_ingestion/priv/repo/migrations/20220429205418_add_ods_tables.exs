defmodule :"Elixir.ExCubicOdsIngestion.Repo.Migrations.Add ODS tables" do
  use Ecto.Migration

  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsTable

  @tables [
    %CubicOdsTable{
      name: "cubic_ods_qlik__cch_stage_apportionment_rules",
      s3_prefix: "cubic_ods_qlik/CCH_STAGE.APPORTIONMENT_RULES/",
      snapshot_s3_key: "cubic_ods_qlik/CCH_STAGE.APPORTIONMENT_RULES/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_abp_tap",
      s3_prefix: "cubic_ods_qlik/EDW.ABP_TAP/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.ABP_TAP/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_activity_code_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.ACTIVITY_CODE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.ACTIVITY_CODE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_adjustment_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.ADJUSTMENT_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.ADJUSTMENT_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_approval_status_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.APPROVAL_STATUS_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.APPROVAL_STATUS_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_b2b_order_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.B2B_ORDER_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.B2B_ORDER_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_bankcard_payment_typ_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.BANKCARD_PAYMENT_TYP_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.BANKCARD_PAYMENT_TYP_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_be_invoice_status_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.BE_INVOICE_STATUS_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.BE_INVOICE_STATUS_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_benefit_order_status_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.BENEFIT_ORDER_STATUS_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.BENEFIT_ORDER_STATUS_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_benefit_status_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.BENEFIT_STATUS_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.BENEFIT_STATUS_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_bnft_inv_lineitm_typ_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.BNFT_INV_LINEITM_TYP_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.BNFT_INV_LINEITM_TYP_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_bnft_invoice_status_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.BNFT_INVOICE_STATUS_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.BNFT_INVOICE_STATUS_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_bnft_stg_batch_stat_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.BNFT_STG_BATCH_STAT_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.BNFT_STG_BATCH_STAT_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_bnft_stg_detail_stat_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.BNFT_STG_DETAIL_STAT_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.BNFT_STG_DETAIL_STAT_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_business_entity_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.BUSINESS_ENTITY_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.BUSINESS_ENTITY_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_card_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.CARD_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.CARD_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_card_product",
      s3_prefix: "cubic_ods_qlik/EDW.CARD_PRODUCT/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.CARD_PRODUCT/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_card_status",
      s3_prefix: "cubic_ods_qlik/EDW.CARD_STATUS/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.CARD_STATUS/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_cch_gl_summary_import",
      s3_prefix: "cubic_ods_qlik/EDW.CCH_GL_SUMMARY_IMPORT/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.CCH_GL_SUMMARY_IMPORT/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_cm_payment_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.CM_PAYMENT_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.CM_PAYMENT_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_credit_card_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.CREDIT_CARD_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.CREDIT_CARD_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_customer_benefit",
      s3_prefix: "cubic_ods_qlik/EDW.CUSTOMER_BENEFIT/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.CUSTOMER_BENEFIT/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_customer_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.CUSTOMER_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.CUSTOMER_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_date_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.DATE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.DATE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_device_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.DEVICE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.DEVICE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_device_event",
      s3_prefix: "cubic_ods_qlik/EDW.DEVICE_EVENT/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.DEVICE_EVENT/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_device_location_history",
      s3_prefix: "cubic_ods_qlik/EDW.DEVICE_LOCATION_HISTORY/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.DEVICE_LOCATION_HISTORY/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_employee_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.EMPLOYEE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.EMPLOYEE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_event_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.EVENT_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.EVENT_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_fare_prod_users_list_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.FARE_PROD_USERS_LIST_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.FARE_PROD_USERS_LIST_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_fare_product_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.FARE_PRODUCT_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.FARE_PRODUCT_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_fare_product_instance",
      s3_prefix: "cubic_ods_qlik/EDW.FARE_PRODUCT_INSTANCE/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.FARE_PRODUCT_INSTANCE/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_fare_revenue_report_schedule",
      s3_prefix: "cubic_ods_qlik/EDW.FARE_REVENUE_REPORT_SCHEDULE/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.FARE_REVENUE_REPORT_SCHEDULE/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_farerev_recovery_txn",
      s3_prefix: "cubic_ods_qlik/EDW.FAREREV_RECOVERY_TXN/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.FAREREV_RECOVERY_TXN/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_fee_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.FEE_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.FEE_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_frm_bank_fee_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.FRM_BANK_FEE_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.FRM_BANK_FEE_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_frm_crdb_acq_bank_fee",
      s3_prefix: "cubic_ods_qlik/EDW.FRM_CRDB_ACQ_BANK_FEE/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.FRM_CRDB_ACQ_BANK_FEE/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_frm_merchant_mapping",
      s3_prefix: "cubic_ods_qlik/EDW.FRM_MERCHANT_MAPPING/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.FRM_MERCHANT_MAPPING/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_journal_entry",
      s3_prefix: "cubic_ods_qlik/EDW.JOURNAL_ENTRY/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.JOURNAL_ENTRY/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_journal_entry_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.JOURNAL_ENTRY_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.JOURNAL_ENTRY_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_kpi",
      s3_prefix: "cubic_ods_qlik/EDW.KPI/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.KPI/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_kpi_detail_events_by_day",
      s3_prefix: "cubic_ods_qlik/EDW.KPI_DETAIL_EVENTS_BY_DAY/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.KPI_DETAIL_EVENTS_BY_DAY/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_kpi_rule",
      s3_prefix: "cubic_ods_qlik/EDW.KPI_RULE/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.KPI_RULE/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_kpi_summary_by_day",
      s3_prefix: "cubic_ods_qlik/EDW.KPI_SUMMARY_BY_DAY/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.KPI_SUMMARY_BY_DAY/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_media_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.MEDIA_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.MEDIA_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_member_benefit",
      s3_prefix: "cubic_ods_qlik/EDW.MEMBER_BENEFIT/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.MEMBER_BENEFIT/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_metric_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.METRIC_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.METRIC_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_metric_summary_by_day",
      s3_prefix: "cubic_ods_qlik/EDW.METRIC_SUMMARY_BY_DAY/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.METRIC_SUMMARY_BY_DAY/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_mo_fulfill_status_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.MO_FULFILL_STATUS_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.MO_FULFILL_STATUS_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_mo_line_item_status_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.MO_LINE_ITEM_STATUS_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.MO_LINE_ITEM_STATUS_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_mo_order_li_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.MO_ORDER_LI_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.MO_ORDER_LI_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_mo_order_status_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.MO_ORDER_STATUS_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.MO_ORDER_STATUS_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_oa_authority_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.OA_AUTHORITY_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.OA_AUTHORITY_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_operator_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.OPERATOR_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.OPERATOR_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_pal_confirmation",
      s3_prefix: "cubic_ods_qlik/EDW.PAL_CONFIRMATION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.PAL_CONFIRMATION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_patron_account_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.PATRON_ACCOUNT_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.PATRON_ACCOUNT_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_patron_order",
      s3_prefix: "cubic_ods_qlik/EDW.PATRON_ORDER/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.PATRON_ORDER/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_patron_order_line_item",
      s3_prefix: "cubic_ods_qlik/EDW.PATRON_ORDER_LINE_ITEM/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.PATRON_ORDER_LINE_ITEM/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_patron_order_payment",
      s3_prefix: "cubic_ods_qlik/EDW.PATRON_ORDER_PAYMENT/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.PATRON_ORDER_PAYMENT/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_patron_trip",
      s3_prefix: "cubic_ods_qlik/EDW.PATRON_TRIP/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.PATRON_TRIP/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_patronage_summary",
      s3_prefix: "cubic_ods_qlik/EDW.PATRONAGE_SUMMARY/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.PATRONAGE_SUMMARY/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_payment_summary",
      s3_prefix: "cubic_ods_qlik/EDW.PAYMENT_SUMMARY/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.PAYMENT_SUMMARY/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_payment_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.PAYMENT_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.PAYMENT_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_read_transaction",
      s3_prefix: "cubic_ods_qlik/EDW.READ_TRANSACTION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.READ_TRANSACTION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_read_txn_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.READ_TXN_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.READ_TXN_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_reason_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.REASON_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.REASON_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_ride_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.RIDE_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.RIDE_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_rider_class_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.RIDER_CLASS_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.RIDER_CLASS_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_route_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.ROUTE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.ROUTE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_route_stop_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.ROUTE_STOP_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.ROUTE_STOP_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_sale_transaction",
      s3_prefix: "cubic_ods_qlik/EDW.SALE_TRANSACTION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SALE_TRANSACTION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_sale_txn_payment",
      s3_prefix: "cubic_ods_qlik/EDW.SALE_TXN_PAYMENT/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SALE_TXN_PAYMENT/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_sale_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.SALE_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SALE_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_sales_channel_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.SALES_CHANNEL_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SALES_CHANNEL_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_sample",
      s3_prefix: "cubic_ods_qlik/EDW.SAMPLE/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SAMPLE/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_service_type_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.SERVICE_TYPE_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SERVICE_TYPE_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_stop_point_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.STOP_POINT_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.STOP_POINT_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_alm_hardware",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_ALM_HARDWARE/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_ALM_HARDWARE/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_alm_stockroom",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_ALM_STOCKROOM/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_ALM_STOCKROOM/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_cmdb_ci",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_CMDB_CI/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_CMDB_CI/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_cmdb_ci_outage",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_CMDB_CI_OUTAGE/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_CMDB_CI_OUTAGE/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_cmn_location",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_CMN_LOCATION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_CMN_LOCATION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_contract_sla",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_CONTRACT_SLA/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_CONTRACT_SLA/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_core_company",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_CORE_COMPANY/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_CORE_COMPANY/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_sc_req_item",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_SC_REQ_ITEM/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_SC_REQ_ITEM/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_sc_request",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_SC_REQUEST/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_SC_REQUEST/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_sm_asset_usage",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_SM_ASSET_USAGE/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_SM_ASSET_USAGE/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_task_sla",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_TASK_SLA/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_TASK_SLA/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_u_cmdb_ci_vehicle",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_U_CMDB_CI_VEHICLE/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_U_CMDB_CI_VEHICLE/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_u_fs_action_codes",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_U_FS_ACTION_CODES/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_U_FS_ACTION_CODES/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_u_fs_chargeability_codes",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_U_FS_CHARGEABILITY_CODES/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_U_FS_CHARGEABILITY_CODES/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_u_fs_event_code",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_U_FS_EVENT_CODE/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_U_FS_EVENT_CODE/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_u_fs_fault_codes",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_U_FS_FAULT_CODES/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_U_FS_FAULT_CODES/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_u_fs_faulty_items",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_U_FS_FAULTY_ITEMS/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_U_FS_FAULTY_ITEMS/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_u_fs_rpir_code_rt_cause_id",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_U_FS_RPIR_CODE_RT_CAUSE_ID/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_U_FS_RPIR_CODE_RT_CAUSE_ID/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_u_kpi_level",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_U_KPI_LEVEL/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_U_KPI_LEVEL/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_wm_order",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_WM_ORDER/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_WM_ORDER/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_svn_wm_task",
      s3_prefix: "cubic_ods_qlik/EDW.SVN_WM_TASK/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SVN_WM_TASK/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_time_increment_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.TIME_INCREMENT_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.TIME_INCREMENT_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_time_period_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.TIME_PERIOD_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.TIME_PERIOD_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_transaction_history",
      s3_prefix: "cubic_ods_qlik/EDW.TRANSACTION_HISTORY/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.TRANSACTION_HISTORY/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_transit_account_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.TRANSIT_ACCOUNT_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.TRANSIT_ACCOUNT_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_trip_payment",
      s3_prefix: "cubic_ods_qlik/EDW.TRIP_PAYMENT/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.TRIP_PAYMENT/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_txn_channel_map",
      s3_prefix: "cubic_ods_qlik/EDW.TXN_CHANNEL_MAP/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.TXN_CHANNEL_MAP/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_txn_desc_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.TXN_DESC_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.TXN_DESC_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_txn_status_dimension",
      s3_prefix: "cubic_ods_qlik/EDW.TXN_STATUS_DIMENSION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.TXN_STATUS_DIMENSION/LOAD00000001.csv.gz"
    },
    %CubicOdsTable{
      name: "cubic_ods_qlik__edw_use_transaction",
      s3_prefix: "cubic_ods_qlik/EDW.USE_TRANSACTION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.USE_TRANSACTION/LOAD00000001.csv.gz"
    },
  ]

  def up do
    Repo.transaction(fn ->
      Enum.each(@tables, fn table ->
        Repo.insert!(table)
      end)
    end)
  end

  def down do
    Repo.transaction(fn ->
      Enum.each(@tables, fn table ->
        Repo.delete!(Repo.get_by!(CubicOdsTable, name: table.name))
      end)
    end)
  end
end



