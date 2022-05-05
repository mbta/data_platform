defmodule ExCubicIngestion.Repo.Migrations.AddSampleAndUseTransactionTables do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable

  def up do
    Repo.insert!(%CubicTable{
      name: "cubic_ods_qlik__edw_sample",
      s3_prefix: "cubic/ods_qlik/EDW.SAMPLE/"
    })

    Repo.insert!(%CubicTable{
      name: "cubic_ods_qlik__edw_use_transaction",
      s3_prefix: "cubic/ods_qlik/EDW.USE_TRANSACTION/"
    })
  end

  def down do
    Repo.delete!(Repo.get_by!(CubicTable, name: "cubic_ods_qlik__edw_sample"))
    Repo.delete!(Repo.get_by!(CubicTable, name: "cubic_ods_qlik__edw_use_transaction"))
  end
end
