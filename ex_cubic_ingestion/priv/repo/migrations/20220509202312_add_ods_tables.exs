defmodule ExCubicIngestion.Repo.Migrations.AddOdsTables do
  use Ecto.Migration

  def up do
    create table(:cubic_ods_table_snapshots, primary_key: false) do
      add :id, :bigserial, primary_key: true
      add :table_id, :bigint, null: false
      add :snapshot, :utc_datetime
      add :snapshot_s3_key, :string, size: 1000, null: false

      add :deleted_at, :utc_datetime

      timestamps(type: :utc_datetime)
    end

    create table(:cubic_ods_load_snapshots, primary_key: false) do
      add :id, :bigserial, primary_key: true
      add :load_id, :bigint, null: false
      add :snapshot, :utc_datetime

      add :deleted_at, :utc_datetime

      timestamps(type: :utc_datetime)
    end
  end

  def down do
    drop table(:cubic_ods_table_snapshots)
    drop table(:cubic_ods_load_snapshots)
  end
end
