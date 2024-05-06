defmodule Elixir.ExCubicIngestion.Repo.Migrations.MakeKpiMonthlySldcTableActive do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_table_name "cubic_ods_qlik__edw_kpi_monthly_sldc"
  @ods_table_s3_prefix "cubic/ods_qlik/EDW.KPI_MONTHLY_SLDC/"

  def up do
    ods_table_rec = CubicTable.get_by!(name: @ods_table_name)
    ods_table_rec = Ecto.Changeset.change(ods_table_rec, is_active: true)
    Repo.update!(ods_table_rec)
  end

  def down do
    ods_table_rec = CubicTable.get_by!(name: @ods_table_name)
    ods_table_rec = Ecto.Changeset.change(ods_table_rec, is_active: nil)
    Repo.update!(ods_table_rec)
  end
end
