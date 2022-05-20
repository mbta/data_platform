defmodule ExCubicIngestion.Repo.Migrations.CreateIngestionTables do
  use Ecto.Migration

  def up do
    create table(:cubic_tables, primary_key: false) do
      add :id, :bigserial, primary_key: true
      add :name, :string, size: 500, null: false
      add :s3_prefix, :string, size: 1000, null: false

      add :deleted_at, :utc_datetime

      timestamps(type: :utc_datetime)
    end

    create unique_index("cubic_tables", :name)
    create unique_index("cubic_tables", :s3_prefix)

    create table(:cubic_loads, primary_key: false) do
      add :id, :bigserial, primary_key: true
      add :table_id, :bigint, null: false
      add :status, :string, size: 100, null: false
      add :s3_key, :string, size: 1000, null: false
      add :s3_modified, :utc_datetime, null: false
      add :s3_size, :bigint, null: false

      add :deleted_at, :utc_datetime

      timestamps(type: :utc_datetime)
    end
  end

  def down do
    drop table(:cubic_tables)
    drop table(:cubic_loads)
  end
end
