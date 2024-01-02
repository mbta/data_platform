defmodule ExCubicIngestion.Repo.Migrations.AddIsActiveForCubicTables do
  use Ecto.Migration

  import Ecto.Query

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable

  def up do
    alter table(:cubic_tables) do
      add :is_active, :boolean
    end

    flush()

    Repo.update_all(from(table in CubicTable, where: is_nil(table.deleted_at)), set: [is_active: true])
  end

  def down do
    alter table(:cubic_tables) do
      remove :is_active
    end
  end
end
