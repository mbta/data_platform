defmodule ExCubicIngestion.Repo.Migrations.AddIsRawForTablesLoads do
  use Ecto.Migration

  import Ecto.Query

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicTable

  def up do
    alter table(:cubic_tables) do
      add :is_raw, :boolean
    end

    alter table(:cubic_loads) do
      add :is_raw, :boolean
    end

    flush()

    Repo.update_all(from(table in CubicTable, where: is_nil(table.deleted_at)), set: [is_raw: true])
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
