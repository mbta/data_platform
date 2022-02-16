defmodule ExCubicOdsIngestion.Schema.CubicOdsTable do
  @moduledoc """
  Schema.CubicOdsTable @todo
  """
  use Ecto.Schema

  alias ExCubicOdsIngestion.Repo

  import Ecto.Query
  import Ecto.Changeset

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
    field(:s3_prefix, :string)
    field(:snapshot, :utc_datetime)
    field(:snapshot_s3_key, :string)

    field(:deleted_at, :utc_datetime)

    timestamps(type: :utc_datetime)
  end

  @spec get_all :: [t()]
  def get_all do
    # @todo add deleted filter
    query =
      from(table in __MODULE__)

    Repo.all(query)
  end

  def update_snapshot(table_rec, load_rec) do
    if table_rec do
      if table_rec.snapshot_s3_key == load_rec.s3_key and table_rec.snapshot != load_rec.s3_modified do
        Repo.transaction(fn ->
          Repo.update!(change(table_rec, snapshot: load_rec.s3_modified))
        end)
      end
    else
      nil
    end
  end
end
