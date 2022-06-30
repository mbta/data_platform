defmodule ExCubicIngestion.Repo.Migrations.AddIsRawForTablesLoads do
  use Ecto.Migration

  def up do
    alter table(:cubic_tables) do
      add :is_raw, :boolean
    end

    alter table(:cubic_loads) do
      add :is_raw, :boolean
    end
  end

  def down do
    alter table(:cubic_tables) do
      remove :is_raw
    end

    alter table(:cubic_loads) do
      remove :is_raw
    end
  end
end
