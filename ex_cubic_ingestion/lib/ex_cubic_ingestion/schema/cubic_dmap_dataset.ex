defmodule ExCubicIngestion.Schema.CubicDmapDataset do
  @moduledoc """
  Contains a list of DMAP datasets as returned by a DMAP feed. Each dataset is
  processed by the Fetch DMAP worker.
  """
  use Ecto.Schema

  import Ecto.Query

  alias Ecto.Changeset
  alias ExCubicIngestion.Repo

  @derive {Jason.Encoder,
           only: [
             :id,
             :feed_id,
             :type,
             :identifier,
             :start_date,
             :end_date,
             :last_updated_at,
             :deleted_at,
             :inserted_at,
             :updated_at
           ]}

  @type t :: %__MODULE__{
          id: integer() | nil,
          feed_id: integer(),
          type: String.t(),
          identifier: String.t(),
          start_date: Date.t(),
          end_date: Date.t(),
          last_updated_at: DateTime.t(),
          deleted_at: DateTime.t() | nil,
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  schema "cubic_dmap_datasets" do
    field(:feed_id, :integer)
    field(:type, :string)
    field(:identifier, :string)
    field(:start_date, :date)
    field(:end_date, :date)
    field(:last_updated_at, :utc_datetime_usec)

    field(:deleted_at, :utc_datetime)

    timestamps(type: :utc_datetime)
  end

  @spec not_deleted :: Ecto.Queryable.t()
  defp not_deleted do
    from(dmap_dataset in __MODULE__, where: is_nil(dmap_dataset.deleted_at))
  end

  @spec get(integer()) :: t()
  def get(id) do
    Repo.get(not_deleted(), id)
  end

  @spec get!(integer()) :: t()
  def get!(id) do
    Repo.get!(not_deleted(), id)
  end

  @spec get_by(Keyword.t() | map(), Keyword.t()) :: t() | nil
  def get_by(clauses, opts \\ []) do
    Repo.get_by(not_deleted(), clauses, opts)
  end

  @spec get_by!(Keyword.t() | map(), Keyword.t()) :: t() | nil
  def get_by!(clauses, opts \\ []) do
    Repo.get_by!(not_deleted(), clauses, opts)
  end

  @doc """
  For a list of datasets (json blob), upsert to database and return records
  """
  @spec upsert_many_from_datasets(map(), t()) :: [t()]
  def upsert_many_from_datasets(datasets, feed_rec) do
    {:ok, recs} =
      Repo.transaction(fn ->
        Enum.map(datasets, &upsert_from_dataset(&1, feed_rec))
      end)

    # return records
    recs
  end

  @spec upsert_from_dataset(map(), CubicDmapFeed.t()) :: t()
  defp upsert_from_dataset(dataset, feed_rec) do
    rec = get_by(identifier: dataset["dataset_id"])

    if rec do
      Repo.update!(
        Changeset.change(rec, %{
          last_updated_at: iso_extended_to_datetime(dataset["last_updated"])
        })
      )
    else
      Repo.insert!(%__MODULE__{
        feed_id: feed_rec.id,
        type: dataset["id"],
        identifier: dataset["dataset_id"],
        start_date: Date.from_iso8601!(dataset["start_date"]),
        end_date: Date.from_iso8601!(dataset["end_date"]),
        last_updated_at: iso_extended_to_datetime(dataset["last_updated"])
      })
    end
  end

  @spec iso_extended_to_datetime(String.t()) :: DateTime.t()
  defp iso_extended_to_datetime(iso_extended) do
    iso_extended
      |> Timex.parse!("{ISO:Extended}")
      |> DateTime.from_naive!("Etc/UTC")
  end
end
