defmodule ExCubicIngestion.Repo.Migrations.AddIndexesForLoads do
  use Ecto.Migration

  def up do
    create index("cubic_loads", [:status, :deleted_at])
    create index("cubic_loads", [:s3_key, :s3_modified, :deleted_at])
    create index("cubic_ods_load_snapshots", [:load_id, :deleted_at])
  end

  def down do
    drop index("cubic_ods_load_snapshots", [:load_id, :deleted_at])
    drop index("cubic_loads", [:s3_key, :s3_modified, :deleted_at])
    drop index("cubic_loads", [:status, :deleted_at])
  end
end
