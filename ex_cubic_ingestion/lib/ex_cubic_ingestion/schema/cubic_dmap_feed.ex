defmodule ExCubicIngestion.Schema.CubicDmapFeed do
  @moduledoc """
  Contains a list of DMAP feeds. These feeds will be scheduled to be fetched by
  the ScheduleDmap worker.
  """
  use Ecto.Schema

  import Ecto.Query

  alias Ecto.Changeset
  alias ExCubicIngestion.Repo

  @derive {Jason.Encoder,
           only: [
             :id,
             :relative_url,
             :last_updated_at,
             :deleted_at,
             :inserted_at,
             :updated_at
           ]}

  @type t :: %__MODULE__{
          id: integer() | nil,
          relative_url: String.t(),
          last_updated_at: DateTime.t() | nil,
          deleted_at: DateTime.t() | nil,
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  schema "cubic_dmap_feeds" do
    field(:relative_url, :string)
    field(:last_updated_at, :utc_datetime_usec)

    field(:deleted_at, :utc_datetime)

    timestamps(type: :utc_datetime)
  end

  @spec not_deleted :: Ecto.Queryable.t()
  defp not_deleted do
    from(dmap_feed in __MODULE__, where: is_nil(dmap_feed.deleted_at))
  end

  @spec get!(integer()) :: t()
  def get!(id) do
    Repo.get!(not_deleted(), id)
  end

  @doc """
  Finds the dataset that was last updated and updates the feed's last updated value
  """
  @spec update_last_updated_from_datasets([CubicDmapDataset.t()], t()) :: t()
  def update_last_updated_from_datasets(dataset_recs, rec) do
    [latest_updated_dataset_rec | _rest] = Enum.sort_by(dataset_recs, & &1.last_updated_at, :desc)

    Repo.update!(
      Changeset.change(rec, %{
        last_updated_at: latest_updated_dataset_rec.last_updated_at
      })
    )
  end
end
