defmodule ExCubicIngestion.Repo.Migrations.AddSampleDmapTable do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable

  def up do
    Repo.insert!(%CubicTable{
      name: "cubic_dmap__agg_sample",
      s3_prefix: "cubic/dmap/agg_sample/",
      is_raw: true
    })
  end

  def down do
    Repo.delete!(CubicTable.get_by!(
      name: "cubic_dmap__agg_sample"
    ))
  end
end
