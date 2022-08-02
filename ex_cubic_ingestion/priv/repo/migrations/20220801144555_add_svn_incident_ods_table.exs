defmodule ExCubicIngestion.Repo.Migrations.AddSvnIncidentOdsTable do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  def up do
    ods_edw_svn_incident_table_rec = Repo.insert!(%CubicTable{
      name: "cubic_ods_qlik__edw_svn_incident",
      s3_prefix: "cubic/ods_qlik/EDW.SVN_INCIDENT/",
      is_raw: true
    })
    Repo.insert!(%CubicOdsTableSnapshot{
      table_id: ods_edw_svn_incident_table_rec.id,
      snapshot_s3_key: "cubic/ods_qlik/EDW.SVN_INCIDENT/LOAD00000001.csv.gz"
    })
  end

  def down do
    ods_edw_svn_incident_table_rec = CubicTable.get_by!(name: "cubic_ods_qlik__edw_svn_incident")
    Repo.delete!(ods_edw_svn_incident_table_rec)
    Repo.delete!(CubicOdsTableSnapshot.get_by!(table_id: ods_edw_svn_incident_table_rec.id))
  end
end
