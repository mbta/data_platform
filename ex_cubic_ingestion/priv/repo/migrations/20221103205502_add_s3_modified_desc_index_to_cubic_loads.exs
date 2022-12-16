defmodule ExCubicIngestion.Repo.Migrations.AddS3ModifiedDescIndexToCubicLoads do
  use Ecto.Migration

  def up do
    create index("cubic_loads", ["s3_modified DESC"])
  end

  def down do
    drop index("cubic_loads", ["s3_modified DESC"])
  end
end
