defmodule ExCubicIngestion.Repo.Migrations.AddSampleAndUseTransactionTables do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  def up do
    edw_sample_table_rec = Repo.insert!(%CubicTable{
      name: "cubic_ods_qlik__edw_sample",
      s3_prefix: "cubic/ods_qlik/EDW.SAMPLE/"
    })
    Repo.insert!(%CubicOdsTableSnapshot{
      table_id: edw_sample_table_rec.id,
      snapshot_s3_key: "cubic/ods_qlik/EDW.SAMPLE/LOAD1.csv"
    })

    edw_use_transaction_table_rec = Repo.insert!(%CubicTable{
      name: "cubic_ods_qlik__edw_use_transaction",
      s3_prefix: "cubic/ods_qlik/EDW.USE_TRANSACTION/"
    })
    Repo.insert!(%CubicOdsTableSnapshot{
      table_id: edw_use_transaction_table_rec.id,
      snapshot_s3_key: "cubic/ods_qlik/EDW.USE_TRANSACTION/LOAD00000001.csv.gz"
    })
  end

  def down do
    edw_sample_table_rec = CubicTable.get_by!(name: "cubic_ods_qlik__edw_sample")
    Repo.delete!(edw_sample_table_rec)
    Repo.delete!(CubicOdsTableSnapshot.get_by!(table_id: edw_sample_table_rec.id))

    edw_use_transaction_table_rec = CubicTable.get_by!(name: "cubic_ods_qlik__edw_sample")
    Repo.delete!(edw_use_transaction_table_rec)
    Repo.delete!(CubicOdsTableSnapshot.get_by!(table_id: edw_use_transaction_table_rec.id))
  end
end
