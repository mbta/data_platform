defmodule ExCubicOdsIngestion.Schema.CubicOdsLoad do
  @moduledoc """
  Schema.CubicOdsLoad @todo
  """
  use Ecto.Schema

  import Ecto.Query

  alias ExCubicOdsIngestion.Repo

  @type t :: %__MODULE__{
          id: integer() | nil,
          table_id: integer() | nil,
          status: String.t() | nil,
          snapshot: DateTime.t() | nil,
          is_cdc: boolean() | nil,
          s3_key: String.t() | nil,
          s3_modified: DateTime.t() | nil,
          s3_size: integer() | nil,
          deleted_at: DateTime.t() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  schema "cubic_ods_loads" do
    field(:table_id, :integer)
    field(:status, :string)
    field(:snapshot, :utc_datetime)
    field(:is_cdc, :boolean)
    field(:s3_key, :string)
    field(:s3_modified, :utc_datetime)
    field(:s3_size, :integer)

    field(:deleted_at, :utc_datetime)

    timestamps(type: :utc_datetime)
  end

  @spec insert_from_objects(list()) :: tuple()
  def insert_from_objects(objects) do
    Repo.transaction(fn -> Enum.map(objects, &insert_ready(&1)) end)
  end

  @spec insert_ready(map()) :: Ecto.Schema.t()
  def insert_ready(object) do
    {:ok, last_modified_with_msec, _offset} = DateTime.from_iso8601(object[:last_modified])
    last_modified = DateTime.truncate(last_modified_with_msec, :second)
    size = String.to_integer(object[:size])

    Repo.insert!(%__MODULE__{
      status: "ready",
      s3_key: object[:key],
      s3_modified: last_modified,
      s3_size: size
    })
  end

  @spec get_by_objects(list()) :: [t()]
  def get_by_objects(objects) do
    # put together filters based on the object info
    filters =
      Enum.map(objects, fn object ->
        {:ok, last_modified_with_msec, _offset} = DateTime.from_iso8601(object[:last_modified])
        last_modified = DateTime.truncate(last_modified_with_msec, :second)

        {object[:key], last_modified}
      end)

    # we only want to query if we have filters because otherwise the query will the return
    # the whole table
    if Enum.empty?(filters) do
      []
    else
      query_with_filters =
        Enum.reduce(filters, __MODULE__, fn {s3_key, s3_modified}, query ->
          from(load in query,
            or_where: load.s3_key == ^s3_key and load.s3_modified == ^s3_modified
          )
        end)

      Repo.all(query_with_filters)
    end
  end

  @spec get_status_ready :: [t()]
  def get_status_ready do
    # @todo add deleted filter
    query =
      from(load in __MODULE__,
        where: load.status == "ready",
        order_by: [load.s3_modified, load.s3_key]
      )

    Repo.all(query)
  end
end
