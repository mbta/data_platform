defmodule ExCubicOdsIngestion.Repo.Migrations.CubicOdsLoadTableNotNull do
  use Ecto.Migration

  def change do
    alter table(:cubic_ods_loads) do
      modify(:table_id, :bigint, null: false, from: :bigint)
    end
  end
end
