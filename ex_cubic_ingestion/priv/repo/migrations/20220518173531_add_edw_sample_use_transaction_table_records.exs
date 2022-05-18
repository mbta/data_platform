defmodule ExCubicIngestion.Repo.Migrations.AddEdwSampleUseTransactionTableRecords do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  def up do
    ods_edw_sample_table_rec = Repo.insert!(%CubicTable{
      name: "cubic_ods_qlik__edw_sample",
      s3_prefix: "cubic/ods_qlik/EDW.SAMPLE/"
    })
    Repo.insert!(%CubicOdsTableSnapshot{
      table_id: ods_edw_sample_table_rec.id,
      snapshot_s3_key: "cubic/ods_qlik/EDW.SAMPLE/LOAD1.csv"
    })

    ods_edw_use_transaction_table_rec = Repo.insert!(%CubicTable{
      name: "cubic_ods_qlik__edw_use_transaction",
      s3_prefix: "cubic/ods_qlik/EDW.USE_TRANSACTION/"
    })
    Repo.insert!(%CubicOdsTableSnapshot{
      table_id: ods_edw_use_transaction_table_rec.id,
      snapshot_s3_key: "cubic/ods_qlik/EDW.USE_TRANSACTION/LOAD00000001.csv.gz"
    })
  end

  def down do
    ods_edw_sample_table_rec = CubicTable.get_by!(name: "cubic_ods_qlik__edw_sample")
    Repo.delete!(ods_edw_sample_table_rec)
    Repo.delete!(CubicOdsTableSnapshot.get_by!(table_id: ods_edw_sample_table_rec.id))

    ods_edw_use_transaction_table_rec = CubicTable.get_by!(name: "cubic_ods_qlik__edw_use_transaction")
    Repo.delete!(ods_edw_use_transaction_table_rec)
    Repo.delete!(CubicOdsTableSnapshot.get_by!(table_id: ods_edw_use_transaction_table_rec.id))
  end
end
