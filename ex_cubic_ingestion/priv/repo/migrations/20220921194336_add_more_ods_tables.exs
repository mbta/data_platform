defmodule ExCubicIngestion.Repo.Migrations.AddMoreOdsTables do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_tables [
    %{
      name: "cubic_ods_qlik__edw_cch_afc_transaction",
      s3_prefix: "cubic/ods_qlik/EDW.CCH_AFC_TRANSACTION/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_alm_asset",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_ALM_ASSET/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_alm_transfer_order",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_ALM_TRANSFER_ORDER/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_alm_transfer_order_line",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_ALM_TRANSFER_ORDER_LINE/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_change_request",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_CHANGE_REQUEST/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_change_task",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_CHANGE_TASK/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_cmdb",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_CMDB/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_cmdb_rel_ci",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_CMDB_REL_CI/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_cmdb_rel_type",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_CMDB_REL_TYPE/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_problem",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_PROBLEM/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_sc_task",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_SC_TASK/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_sm_order",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_SM_ORDER/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_sm_task",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_SM_TASK/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_sn_state",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_SN_STATE/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_sys_choice",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_SYS_CHOICE/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_task",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_TASK/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_task_ci",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_TASK_CI/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_rma",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_RMA/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_rma_actual_fault",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_RMA_ACTUAL_FAULT/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_rma_main_work",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_RMA_MAIN_WORK/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_rma_reported_fault",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_RMA_REPORTED_FAULT/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_u_software_versions",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_U_SOFTWARE_VERSIONS/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_ufs_repair_code_categories",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_UFS_REPAIR_CODE_CATEGORIES/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_x_cutrs_cust_e_bus_rte",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_X_CUTRS_CUST_E_BUS_RTE/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_x_cutrs_cust_e_venue",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_X_CUTRS_CUST_E_VENUE/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_x_cutrs_e_cust_event",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_X_CUTRS_E_CUST_EVENT/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_x_cutrs_e_m2m_loc_ven",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_X_CUTRS_E_M2M_LOC_VEN/"
    },
    %{
      name: "cubic_ods_qlik__edw_svn_x_cutrs_ptt_kpi_data",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_X_CUTRS_PTT_KPI_DATA/"
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
