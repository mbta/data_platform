defmodule ExCubicIngestion.Repo.Migrations.AddDmapFeedsDatasetsTables do
  use Ecto.Migration

  def up do
    create table(:cubic_dmap_feeds, primary_key: false) do
      add :id, :bigserial, primary_key: true

      add :relative_url, :string, size: 1000, null: false
      add :last_updated_at, :utc_datetime_usec

      add :deleted_at, :utc_datetime

      timestamps(type: :utc_datetime)
    end

    create table(:cubic_dmap_datasets, primary_key: false) do
      add :id, :bigserial, primary_key: true

      add :feed_id, :bigint, null: false
      add :type, :string, size: 500, null: false
      add :identifier, :string, size: 500, null: false
      add :start_date, :date, null: false
      add :end_date, :date, null: false
      add :last_updated_at, :utc_datetime_usec, null: false

      add :deleted_at, :utc_datetime

      timestamps(type: :utc_datetime)
    end

    create unique_index("cubic_dmap_datasets", :identifier)
  end

  def down do
    drop table(:cubic_dmap_feeds)
    drop table(:cubic_dmap_datasets)
  end
end
