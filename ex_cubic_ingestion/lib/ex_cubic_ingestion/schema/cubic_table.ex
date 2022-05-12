defmodule ExCubicIngestion.Schema.CubicTable do
  @moduledoc """
  Contains a list of prefixes that are allowed to be processed through the 'incoming' S3 bucket.
  The name also identifies the table in the Glue Data Catalog databases.
  """
  use Ecto.Schema

  import Ecto.Query

  alias ExCubicIngestion.Repo

  @derive {Jason.Encoder,
           only: [
             :id,
             :name,
             :s3_prefix,
             :deleted_at,
             :inserted_at,
             :updated_at
           ]}

  @type t :: %__MODULE__{
          id: integer() | nil,
          name: String.t() | nil,
          s3_prefix: String.t() | nil,
          deleted_at: DateTime.t() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  schema "cubic_tables" do
    field(:name, :string)
    field(:s3_prefix, :string)

    field(:deleted_at, :utc_datetime)

    timestamps(type: :utc_datetime)
  end

  @spec not_deleted :: Ecto.Queryable.t()
  defp not_deleted do
    from(table in __MODULE__, where: is_nil(table.deleted_at))
  end

  @spec get!(integer()) :: t()
  def get!(id) do
    Repo.get!(not_deleted(), id)
  end

  @doc """
  Given an enumerable of S3 prefixes, return those prefixes which represent a #{__MODULE__} and their table.
  """
  @spec filter_to_existing_prefixes(Enumerable.t()) :: [{String.t(), t()}]
  def filter_to_existing_prefixes(prefixes) do
    # in order to prevent querying with an empty list, we just return an empty list
    if Enum.empty?(prefixes) do
      []
    else
      # strip any change tracking suffix (in ODS)
      without_change_tracking =
        prefixes
        |> MapSet.new(&String.replace_suffix(&1, "__ct/", "/"))
        |> Enum.to_list()

      query =
        from(table in __MODULE__,
          where: is_nil(table.deleted_at) and table.s3_prefix in ^without_change_tracking
        )

      valid_prefix_map =
        query
        |> Repo.all()
        |> Map.new(&{&1.s3_prefix, &1})

      for prefix <- prefixes,
          short_prefix = String.replace_suffix(prefix, "__ct/", "/"),
          %__MODULE__{} = table <- [Map.get(valid_prefix_map, short_prefix)] do
        {prefix, table}
      end
    end
  end
end
