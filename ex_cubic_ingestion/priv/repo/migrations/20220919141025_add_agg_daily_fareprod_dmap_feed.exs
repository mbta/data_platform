defmodule ExCubicIngestion.Repo.Migrations.AddAggDailyFareprodDmapFeed do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicDmapFeed

  def up do
    Repo.insert!(%CubicDmapFeed{
      relative_url: "/controlledresearchusersapi/aggregations/agg_daily_fareprod",
      last_updated_at: ~U[2022-09-01 00:00:00.000000Z]
    })
  end

  def down do
    Repo.delete!(CubicDmapFeed.get_by!(
      relative_url: "/controlledresearchusersapi/aggregations/agg_daily_fareprod"
    ))
  end
end
