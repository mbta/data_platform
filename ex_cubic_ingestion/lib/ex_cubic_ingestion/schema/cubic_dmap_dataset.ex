defmodule ExCubicIngestion.Schema.CubicDmapDataset do
  @moduledoc """
  Contains a list of DMAP datasets as returned by a DMAP feed. Each dataset is
  processed by the Fetch DMAP worker.
  """
  use Ecto.Schema

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicDmapFeed
  alias ExCubicIngestion.Validators

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

  @doc """
  Make sure that the dataset has all the required fields and has valid data.
  """
  @spec valid_dataset?(map()) :: boolean()
  def valid_dataset?(dataset) do
    Validators.map_has_keys?(dataset, [
      "id",
      "dataset_id",
      "start_date",
      "end_date",
      "last_updated",
      "url"
    ]) &&
      Validators.valid_iso_date?(dataset["start_date"]) &&
      Validators.valid_iso_date?(dataset["end_date"]) &&
      Validators.valid_iso_datetime?(dataset["last_updated"]) &&
      Validators.valid_dmap_dataset_url?(dataset["url"])
  end

  @doc """
  For a list of datasets (json blob), upsert to database and return records with
  the dataset urls for further processing.
  """
  @spec upsert_many_from_datasets([map()], CubicDmapFeed.t()) :: [{t(), String.t()}]
  def upsert_many_from_datasets(datasets, feed_rec) do
    {:ok, recs_with_url} =
      Repo.transaction(fn ->
        Enum.map(datasets, fn dataset ->
          {upsert_from_dataset(dataset, feed_rec), dataset["url"]}
        end)
      end)

    recs_with_url
  end

  @spec upsert_from_dataset(map(), CubicDmapFeed.t()) :: t()
  defp upsert_from_dataset(dataset, feed_rec) do
    Repo.insert!(
      %__MODULE__{
        feed_id: feed_rec.id,
        type: dataset["id"],
        identifier: dataset["dataset_id"],
        start_date: Date.from_iso8601!(dataset["start_date"]),
        end_date: Date.from_iso8601!(dataset["end_date"]),
        last_updated_at: iso_extended_to_datetime(dataset["last_updated"])
      },
      on_conflict: [set: [last_updated_at: iso_extended_to_datetime(dataset["last_updated"])]],
      conflict_target: :identifier
    )
  end

  @spec iso_extended_to_datetime(String.t()) :: DateTime.t()
  defp iso_extended_to_datetime(iso_extended) do
    iso_extended
    |> Timex.parse!("{ISO:Extended}")
    |> DateTime.from_naive!("Etc/UTC")
  end
end
