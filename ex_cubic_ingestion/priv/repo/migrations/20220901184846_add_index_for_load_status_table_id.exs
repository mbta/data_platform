defmodule ExCubicIngestion.Repo.Migrations.AddIndexForLoadStatusTableId do
  use Ecto.Migration

  def up do
    create index("cubic_loads", [:status, :table_id, :deleted_at])
  end

  def down do
    drop index("cubic_loads", [:status, :table_id, :deleted_at])
  end
end
