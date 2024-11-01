defmodule ExCubicIngestion.Repo.Migrations.AddTransactionOriginDimensionTable do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @ods_table_name "cubic_ods_qlik__edw_transaction_origin_dimension"
  @ods_table_s3_prefix "cubic/ods_qlik/EDW.TRANSACTION_ORIGIN_DIMENSION/"

  def up do
    ods_table_rec = Repo.insert!(%CubicTable{
      name: @ods_table_name,
      s3_prefix: @ods_table_s3_prefix,
      is_active: true,
      is_raw: true
    })
    Repo.insert!(%CubicOdsTableSnapshot{
      table_id: ods_table_rec.id,
      snapshot_s3_key: "#{@ods_table_s3_prefix}LOAD00000001.csv.gz"
    })
  end

  def down do
    ods_table_rec = CubicTable.get_by!(name: @ods_table_name)
    Repo.delete!(ods_table_rec)
    Repo.delete!(CubicOdsTableSnapshot.get_by!(table_id: ods_table_rec.id))
  end
end
