defmodule ExCubicIngestion.Repo.Migrations.UpdateDmapPaths do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicDmapFeed

  @dmap_feeds [
    %{
      old_relative_url: "/controlledresearchusersapi/aggregations/agg_average_boardings_by_day_type_month",
      new_relative_url: "/datasetpublicusersapi/aggregations/agg_average_boardings_by_day_type_month"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/aggregations/agg_boardings_fareprod_mode_month",
      new_relative_url: "/datasetpublicusersapi/aggregations/agg_boardings_fareprod_mode_month"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/aggregations/agg_total_boardings_month_mode",
      new_relative_url: "/datasetpublicusersapi/aggregations/agg_total_boardings_month_mode"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/aggregations/agg_hourly_entry_exit_count",
      new_relative_url: "/datasetpublicusersapi/aggregations/agg_hourly_entry_exit_count"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/aggregations/agg_daily_fareprod_station",
      new_relative_url: "/datasetpublicusersapi/aggregations/agg_daily_fareprod_station"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/aggregations/agg_daily_transfers_station",
      new_relative_url: "/datasetpublicusersapi/aggregations/agg_daily_transfers_station"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/aggregations/agg_daily_transfers_route",
      new_relative_url: "/datasetpublicusersapi/aggregations/agg_daily_transfers_route"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/aggregations/agg_daily_fareprod_route",
      new_relative_url: "/datasetpublicusersapi/aggregations/agg_daily_fareprod_route"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/aggregations/agg_daily_fareprod",
      new_relative_url: "/datasetpublicusersapi/aggregations/agg_daily_fareprod"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/transactional/use_transaction_location",
      new_relative_url: "/datasetcontrolleduserapi/transactional/use_transaction_location"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/transactional/use_transaction_longitudinal",
      new_relative_url: "/datasetcontrolleduserapi/transactional/use_transaction_longitudinal"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/transactional/sale_transaction",
      new_relative_url: "/datasetcontrolleduserapi/transactional/sale_transaction"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/transactional/device_event",
      new_relative_url: "/datasetcontrolleduserapi/transactional/device_event"
    },
    %{
      old_relative_url: "/controlledresearchusersapi/transactional/citation",
      new_relative_url: "/datasetcontrolleduserapi/transactional/citation"
    }
  ]

  def up do
    Repo.transaction(fn ->
      Enum.each(@dmap_feeds, fn dmap_feed ->
        dmap_feed_rec = Repo.get_by!(CubicDmapFeed, relative_url: dmap_feed.old_relative_url)

        Repo.update!(Ecto.Changeset.change(dmap_feed_rec, relative_url: dmap_feed.new_relative_url))
      end)
    end)
  end

  def down do
    Repo.transaction(fn ->
      Enum.each(@dmap_feeds, fn dmap_feed ->
        dmap_feed_rec = Repo.get_by!(CubicDmapFeed, relative_url: dmap_feed.new_relative_url)

        Repo.update!(Ecto.Changeset.change(dmap_feed_rec, relative_url: dmap_feed.old_relative_url))
      end)
    end)
  end
end
