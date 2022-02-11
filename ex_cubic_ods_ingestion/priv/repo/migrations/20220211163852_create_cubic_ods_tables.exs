defmodule ExCubicOdsIngestion.Repo.Migrations.CreateCubicOdsTables do
  use Ecto.Migration

  def up do
    create table(:cubic_ods_tables, primary_key: false) do
      add :id, :bigserial, primary_key: true
      add :name, :string, size: 500, null: false
      add :s3_prefix, :string, size: 1000, null: false
      add :snapshot, :utc_datetime
      add :snapshot_s3_key, :string, size: 1000, null: false

      add :deleted_at, :utc_datetime

      timestamps(type: :utc_datetime)
    end

    create table(:cubic_ods_loads, primary_key: false) do
      add :id, :bigserial, primary_key: true
      add :table_id, :bigint
      add :status, :string, size: 100, null: false
      add :snapshot, :utc_datetime
      add :is_cdc, :boolean
      add :s3_key, :string, size: 1000, null: false
      add :s3_modified, :utc_datetime, null: false
      add :s3_size, :bigint, null: false

      add :deleted_at, :utc_datetime

      timestamps(type: :utc_datetime)
    end
  end

  def down do
    drop table(:cubic_ods_tables)
    drop table(:cubic_ods_loads)
  end
end

