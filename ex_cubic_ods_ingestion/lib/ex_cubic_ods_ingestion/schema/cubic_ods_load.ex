defmodule ExCubicOdsIngestion.Schema.CubicOdsLoad do
  @moduledoc """
  Schema.CubicOdsLoad
  """
  use Ecto.Schema

  @type t :: %__MODULE__{
          id: integer() | nil,
          table_id: integer() | nil,
          status: String.t() | nil,
          snapshot: DateTime.t() | nil,
          is_cdc: boolean() | nil,
          s3_key: String.t() | nil,
          s3_modified: DateTime.t() | nil,
          s3_size: integer() | nil,
          deleted: DateTime.t() | nil,
          created: DateTime.t() | nil,
          modified: DateTime.t() | nil
        }

  schema "cubic_ods_loads" do
    field(:table_id, :integer)
    field(:status, :string)
    field(:snapshot, :utc_datetime)
    field(:is_cdc, :boolean)
    field(:s3_key, :string)
    field(:s3_modified, :utc_datetime)
    field(:s3_size, :integer)

    field(:deleted, :utc_datetime)

    timestamps(
      inserted_at: :created,
      updated_at: :modified,
      type: :utc_datetime
    )
  end
end
