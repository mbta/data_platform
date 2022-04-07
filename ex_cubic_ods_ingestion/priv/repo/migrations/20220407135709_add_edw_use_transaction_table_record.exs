defmodule ExCubicOdsIngestion.Repo.Migrations.AddEdwUseTransactionTableRecord do
  use Ecto.Migration

  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsTable

  @table_name "cubic_ods_qlik__edw_use_transaction"

  def up do
    Repo.insert!(%CubicOdsTable{
      name: @table_name,
      s3_prefix: "cubic_ods_qlik/EDW.USE_TRANSACTION/",
      snapshot_s3_key: "cubic_ods_qlik/EDW.USE_TRANSACTION/LOAD00000001.csv.gz"
    })
  end

  def down do
    Repo.delete!(Repo.get_by!(CubicOdsTable, name: @table_name))
  end
end
