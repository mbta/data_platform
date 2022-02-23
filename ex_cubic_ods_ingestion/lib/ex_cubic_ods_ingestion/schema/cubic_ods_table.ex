defmodule ExCubicOdsIngestion.Schema.CubicOdsTable do
  @moduledoc """
  Schema.CubicOdsTable @todo
  """
  use Ecto.Schema

  alias ExCubicOdsIngestion.Repo

  import Ecto.Query
  import Ecto.Changeset

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

  @spec get(integer()) :: t()
  def get(id) do
    Repo.get!(__MODULE__, id)
  end

  @spec get_from_load_s3_key(String.t()) :: t() | nil
  def get_from_load_s3_key(load_s3_key) do
    # get just the s3 prefix from the key
    load_s3_prefix = Path.dirname(load_s3_key)
    # if cdc, we want to strip off the '__ct'
    load_s3_prefix =
      if String.ends_with?(load_s3_prefix, "__ct") do
        String.replace_suffix(load_s3_prefix, "__ct", "")
      else
        load_s3_prefix
      end

    query =
      from(table in __MODULE__,
        where: table.s3_prefix == ^"#{load_s3_prefix}/"
      )

    # return nil, if not found
    Repo.one(query)
  end

  @spec update(t(), map()) :: {atom(), t()}
  def update(table_rec, changes) do
    {:ok, table_rec} =
      Repo.transaction(fn ->
        Repo.update!(change(table_rec, changes))
      end)

    table_rec
  end
end
