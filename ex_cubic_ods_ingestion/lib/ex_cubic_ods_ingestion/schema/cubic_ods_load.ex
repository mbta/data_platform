defmodule ExCubicOdsIngestion.Schema.CubicOdsLoad do
  @moduledoc """
  Schema.CubicOdsLoad @todo
  """
  use Ecto.Schema

  alias ExCubicOdsIngestion.Repo

  import Ecto.Query

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
    {:ok, last_modified, _offset} = DateTime.from_iso8601(object[:last_modified])
    last_modified = DateTime.truncate(last_modified, :second)
    size = String.to_integer(object[:size])

    Repo.insert!(%__MODULE__{
      status: "ready",
      s3_key: object[:key],
      s3_modified: last_modified,
      s3_size: size
    })
  end

  @spec get_s3_modified_since(DateTime.t()) :: [__MODULE__.t()]
  def get_s3_modified_since(last_modified) do
    IO.puts(last_modified)

    query =
      from(load in __MODULE__,
        where: load.s3_modified >= ^last_modified
      )

    Repo.all(query)
  end
end
