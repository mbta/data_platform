defmodule ExCubicIngestion.Repo.Migrations.UpdateSnapshotKeyForSample do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  def up do
    table_rec = Repo.get_by!(CubicTable, name: "cubic_ods_qlik__edw_sample")
    ods_table_snapshot_rec = Repo.get_by!(CubicOdsTableSnapshot, table_id: table_rec.id)

    Repo.update!(Ecto.Changeset.change(ods_table_snapshot_rec, snapshot_s3_key: "cubic/ods_qlik/EDW.SAMPLE/LOAD1.csv.gz"))
  end

  def down do
    table_rec = Repo.get_by!(CubicTable, name: "cubic_ods_qlik__edw_sample")
    ods_table_snapshot_rec = Repo.get_by!(CubicOdsTableSnapshot, table_id: table_rec.id)

    Repo.update!(Ecto.Changeset.change(ods_table_snapshot_rec, snapshot_s3_key: "cubic/ods_qlik/EDW.SAMPLE/LOAD1.csv"))
  end
end
