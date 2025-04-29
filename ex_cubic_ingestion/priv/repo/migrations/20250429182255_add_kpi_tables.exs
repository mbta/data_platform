defmodule ExCubicIngestion.Repo.Migrations.AddKpiTables do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_table_name_1 "cubic_ods_qlik__edw_kpi_availability_event"
  @ods_table_s3_prefix_1 "cubic/ods_qlik/EDW.KPI_AVAILABILITY_EVENT/"

  @ods_table_name_2 "cubic_ods_qlik__edw_kpi_target"
  @ods_table_s3_prefix_2 "cubic/ods_qlik/EDW.KPI_TARGET/"

  def up do
    ods_table_1_rec = Repo.insert!(%CubicTable{
      name: @ods_table_name_1,
      s3_prefix: @ods_table_s3_prefix_1,
      is_active: true,
      is_raw: true
    })
    Repo.insert!(%CubicOdsTableSnapshot{
      table_id: ods_table_1_rec.id,
      snapshot_s3_key: "#{@ods_table_s3_prefix_1}LOAD00000001.csv.gz"
    })

    ods_table_2_rec = Repo.insert!(%CubicTable{
      name: @ods_table_name_2,
      s3_prefix: @ods_table_s3_prefix_2,
      is_active: true,
      is_raw: true
    })
    Repo.insert!(%CubicOdsTableSnapshot{
      table_id: ods_table_2_rec.id,
      snapshot_s3_key: "#{@ods_table_s3_prefix_2}LOAD00000001.csv.gz"
    })
  end

  def down do
    ods_table_1_rec = CubicTable.get_by!(name: @ods_table_name_1)
    Repo.delete!(ods_table_1_rec)
    Repo.delete!(CubicOdsTableSnapshot.get_by!(table_id: ods_table_1_rec.id))

    ods_table_2_rec = CubicTable.get_by!(name: @ods_table_name_2)
    Repo.delete!(ods_table_2_rec)
    Repo.delete!(CubicOdsTableSnapshot.get_by!(table_id: ods_table_2_rec.id))
  end
end
