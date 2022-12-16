defmodule ExCubicIngestion.Repo.Migrations.AddIndexTableIdForTableSnapshot do
  use Ecto.Migration

  def up do
    create index("cubic_ods_table_snapshots", [:table_id])
  end

  def down do
    drop index("cubic_ods_table_snapshots", [:table_id])
  end
end
