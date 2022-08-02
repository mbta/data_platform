defmodule ExCubicIngestion.Repo.Migrations.AddDmapFeeds do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicDmapFeed
  alias ExCubicIngestion.Schema.CubicTable

  @dmap_tables [
    %{
      relative_url: "/controlledresearchusersapi/aggregations/agg_average_boardings_by_day_type_month",
      name: "cubic_dmap__agg_average_boardings_by_day_type_month",
      s3_prefix: "cubic/dmap/agg_average_boardings_by_day_type_month/"
    },
    %{
      relative_url: "/controlledresearchusersapi/aggregations/agg_boardings_fareprod_mode_month",
      name: "cubic_dmap__agg_boardings_fareprod_mode_month",
      s3_prefix: "cubic/dmap/agg_boardings_fareprod_mode_month/"
    },
    %{
      relative_url: "/controlledresearchusersapi/aggregations/agg_total_boardings_month_mode",
      name: "cubic_dmap__agg_total_boardings_month_mode",
      s3_prefix: "cubic/dmap/agg_total_boardings_month_mode/"
    },
    %{
      relative_url: "/controlledresearchusersapi/aggregations/agg_hourly_entry_exit_count",
      name: "cubic_dmap__agg_hourly_entry_exit_count",
      s3_prefix: "cubic/dmap/agg_hourly_entry_exit_count/"
    },
    %{
      relative_url: "/controlledresearchusersapi/aggregations/agg_daily_fareprod_station",
      name: "cubic_dmap__agg_daily_fareprod_station",
      s3_prefix: "cubic/dmap/agg_daily_fareprod_station/"
    },
    %{
      relative_url: "/controlledresearchusersapi/aggregations/agg_daily_transfers_station",
      name: "cubic_dmap__agg_daily_transfers_station",
      s3_prefix: "cubic/dmap/agg_daily_transfers_station/"
    },
    %{
      relative_url: "/controlledresearchusersapi/aggregations/agg_daily_transfers_route",
      name: "cubic_dmap__agg_daily_transfers_route",
      s3_prefix: "cubic/dmap/agg_daily_transfers_route/"
    },
    %{
      relative_url: "/controlledresearchusersapi/aggregations/agg_daily_fareprod_route",
      name: "cubic_dmap__agg_daily_fareprod_route",
      s3_prefix: "cubic/dmap/agg_daily_fareprod_route/"
    },
    %{
      relative_url: "/controlledresearchusersapi/transactional/use_transaction_longitudinal",
      name: "cubic_dmap__use_transaction_longitudinal",
      s3_prefix: "cubic/dmap/use_transaction_longitudinal/"
    },
    %{
      relative_url: "/controlledresearchusersapi/transactional/sale_transaction",
      name: "cubic_dmap__sale_transaction",
      s3_prefix: "cubic/dmap/sale_transaction/"
    },
    %{
      relative_url: "/controlledresearchusersapi/transactional/device_event",
      name: "cubic_dmap__device_event",
      s3_prefix: "cubic/dmap/device_event/"
    },
    %{
      relative_url: "/controlledresearchusersapi/transactional/citation",
      name: "cubic_dmap__citation",
      s3_prefix: "cubic/dmap/citation/"
    }
  ]

  def up do
    Repo.transaction(fn ->
      Enum.each(@dmap_tables, fn dmap_table ->
        Repo.insert!(%CubicDmapFeed{
          relative_url: dmap_table.relative_url,
          last_updated_at: ~U[2022-06-01 00:00:00.000000Z]
        })

        Repo.insert!(%CubicTable{
          name: dmap_table.name,
          s3_prefix: dmap_table.s3_prefix,
          is_raw: true
        })
      end)
    end)
  end

  def down do
    Repo.transaction(fn ->
      Enum.each(@dmap_tables, fn dmap_table ->
        Repo.delete!(CubicTable.get_by!(
          name: dmap_table.name
        ))

        Repo.delete!(CubicDmapFeed.get_by!(
          relative_url: dmap_table.relative_url
        ))
      end)
    end)
  end
end
