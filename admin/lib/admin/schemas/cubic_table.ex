defmodule Admin.Schema.CubicTable do
  @moduledoc """
  Contains a list of prefixes that are allowed to be processed through the 'incoming' S3 bucket.
  The name also identifies the table in the Glue Data Catalog databases.
  """
  use Ecto.Schema

  @derive {Jason.Encoder,
           only: [
             :id,
             :name,
             :s3_prefix,
             :is_raw,
             :is_active,
             :deleted_at,
             :inserted_at,
             :updated_at
           ]}

  @type t :: %__MODULE__{
          id: integer() | nil,
          name: String.t() | nil,
          s3_prefix: String.t() | nil,
          is_raw: boolean() | nil,
          is_active: boolean() | nil,
          deleted_at: DateTime.t() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  schema "cubic_tables" do
    field(:name, :string)
    field(:s3_prefix, :string)
    field(:is_raw, :boolean)
    field(:is_active, :boolean)

    field(:deleted_at, :utc_datetime)

    timestamps(type: :utc_datetime)
  end
end
