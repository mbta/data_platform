defmodule ExCubicOdsIngestion.Repo.Migrations.AddEdwSampleTableRecord do
  use Ecto.Migration

  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsTable

  @table_name "cubic_ods_qlik__edw_sample"

  def up do
    Repo.insert!(%CubicOdsTable{
      name: @table_name,
      s3_prefix: "cubic_ods_qlik/EDW.SAMPLE/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.SAMPLE/LOAD1.csv"
    })
  end

  def down do
    Repo.delete!(Repo.get_by!(CubicOdsTable, name: @table_name))
  end
end
