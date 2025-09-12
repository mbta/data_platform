defmodule ExCubicIngestion.Repo.Migrations.AddTablesForIngestion do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_tables [
    %{
      name: "cubic_ods_qlik__edw_device_end_of_day_msg_count",
      s3_prefix: "cubic/ods_qlik/EDW.DEVICE_END_OF_DAY_MSG_COUNT/"
    },
    %{
      name: "cubic_ods_qlik__edw_tap_usage_summary",
      s3_prefix: "cubic/ods_qlik/EDW.TAP_USAGE_SUMMARY/"
    },
    %{
      name: "cubic_ods_qlik__edw_vehicle_trip",
      s3_prefix: "cubic/ods_qlik/EDW.VEHICLE_TRIP/"
    },
    %{
      name: "cubic_ods_qlik__edw_facility_dimension",
      s3_prefix: "cubic/ods_qlik/EDW.FACILITY_DIMENSION/"
    },
    %{
      name: "cubic_ods_qlik__edw_kpi_operating_day_schedule",
      s3_prefix: "cubic/ods_qlik/EDW.KPI_OPERATING_DAY_SCHEDULE/"
    },
    %{
      name: "cubic_ods_qlik__edw_citation",
      s3_prefix: "cubic/ods_qlik/EDW.CITATION/"
    },
    %{
      name: "cubic_ods_qlik__edw_kpi_agency_map",
      s3_prefix: "cubic/ods_qlik/EDW.KPI_AGENCY_MAP/"
    }
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
