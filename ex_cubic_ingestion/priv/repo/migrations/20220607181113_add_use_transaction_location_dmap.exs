defmodule ExCubicIngestion.Repo.Migrations.AddUseTransactionDmap do
  use Ecto.Migration

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicDmapFeed
  alias ExCubicIngestion.Schema.CubicTable

  def up do
    create unique_index("cubic_dmap_feeds", :relative_url)

    Repo.insert!(%CubicDmapFeed{
      relative_url: "/controlledresearchusersapi/transactional/use_transaction_location",
      last_updated_at: ~U[2022-06-01 00:00:00.000000Z]
    })

    Repo.insert!(%CubicTable{
      name: "cubic_dmap__use_transaction_location",
      s3_prefix: "cubic/dmap/use_transaction_location/"
    })
  end

  def down do
    Repo.delete!(CubicTable.get_by!(
      name: "cubic_dmap__use_transaction_location"
    ))

    Repo.delete!(CubicDmapFeed.get_by!(
      relative_url: "/controlledresearchusersapi/transactional/use_transaction_location"
    ))

    drop index("cubic_dmap_feeds", [:relative_url])
  end
end
