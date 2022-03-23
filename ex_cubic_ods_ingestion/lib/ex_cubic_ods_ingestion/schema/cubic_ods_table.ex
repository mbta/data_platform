defmodule ExCubicOdsIngestion.Schema.CubicOdsTable do
  @moduledoc """
  Schema.CubicOdsTable @todo
  """
  use Ecto.Schema

  import Ecto.Query
  import Ecto.Changeset

  alias ExCubicOdsIngestion.Repo

  @derive {Jason.Encoder,
           only: [
             :id,
             :name,
             :s3_prefix,
             :snapshot,
             :snapshot_s3_key,
             :deleted_at,
             :inserted_at,
             :updated_at
           ]}

  @type t :: %__MODULE__{
          id: integer() | nil,
          name: String.t() | nil,
          s3_prefix: String.t() | nil,
          snapshot: DateTime.t() | nil,
          snapshot_s3_key: String.t() | nil,
          deleted_at: DateTime.t() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  schema "cubic_ods_tables" do
    field(:name, :string)
    # @todo make this unique
    field(:s3_prefix, :string)
    field(:snapshot, :utc_datetime)
    field(:snapshot_s3_key, :string)

    field(:deleted_at, :utc_datetime)

    timestamps(type: :utc_datetime)
  end

  @spec get!(integer()) :: t()
  def get!(id) do
    Repo.get!(__MODULE__, id)
  end

  @doc """
  Given an enumerable of S3 prefixes, return those prefixes which represent a #{__MODULE__} and their table.
  """
  @spec filter_to_existing_prefixes(Enumerable.t()) :: [{String.t(), t()}]
  def filter_to_existing_prefixes(prefixes) do
    incoming_prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_prefix_incoming)

    # strip incoming bucket prefix as it will not be in the database
    without_incoming_prefix = Enum.map(prefixes, &String.replace_prefix(&1, incoming_prefix, ""))

    # strip any change tracking suffix, and eliminate dups from list
    without_change_tracking =
      without_incoming_prefix
      |> MapSet.new(&replace_change_tracking_suffix(&1))
      |> Enum.to_list()

    query =
      from(table in __MODULE__,
        where: table.s3_prefix in ^without_change_tracking
      )

    valid_prefix_map =
      query
      |> Repo.all()
      |> Map.new(&{&1.s3_prefix, &1})

    for prefix <- without_incoming_prefix,
        short_prefix = replace_change_tracking_suffix(prefix),
        %__MODULE__{} = table <- [Map.get(valid_prefix_map, short_prefix)] do
      {prefix, table}
    end
  end

  @spec update(t(), map()) :: {atom(), t()}
  def update(table_rec, changes) do
    {:ok, table_rec} =
      Repo.transaction(fn ->
        Repo.update!(change(table_rec, changes))
      end)

    table_rec
  end

  @spec replace_change_tracking_suffix(String.t()) :: String.t()
  defp replace_change_tracking_suffix(prefix) do
    String.replace_suffix(prefix, "__ct/", "/")
  end
end
