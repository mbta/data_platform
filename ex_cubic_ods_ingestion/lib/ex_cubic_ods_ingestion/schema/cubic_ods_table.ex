defmodule ExCubicOdsIngestion.Schema.CubicOdsTable do
  @moduledoc """
  Schema.CubicOdsTable
  """
  use Ecto.Schema

  @type t :: %__MODULE__{
          id: integer() | nil,
          name: String.t() | nil,
          s3_prefix: String.t() | nil,
          snapshot: DateTime.t() | nil,
          snapshot_s3_key: String.t() | nil,
          deleted: DateTime.t() | nil,
          created: DateTime.t() | nil,
          modified: DateTime.t() | nil
        }

  schema "cubic_ods_loads" do
    field(:name, :string)
    field(:s3_prefix, :string)
    field(:snapshot, :utc_datetime)
    field(:snapshot_s3_key, :string)

    field(:deleted, :utc_datetime)

    timestamps(
      inserted_at: :created,
      updated_at: :modified,
      type: :utc_datetime
    )
  end
end
