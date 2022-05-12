defmodule ExCubicIngestion.Schema.CubicOdsTableSnapshot do
  @moduledoc """
  ODS tables need to keep track of additional infomation. This table serves for storing
  the snapshot when one is created and uploaded to the 'incoming' S3 bucket.
  The snapshot_s3_key is how we know which object indicates that a new snapshot has been
  created. If this object is passing through, then we need to update our snapshot value.
  """
  use Ecto.Schema

  import Ecto.Query

  alias Ecto.Changeset
  alias ExCubicIngestion.Repo

  @derive {Jason.Encoder,
           only: [
             :id,
             :table_id,
             :snapshot,
             :snapshot_s3_key,
             :deleted_at,
             :inserted_at,
             :updated_at
           ]}

  @type t :: %__MODULE__{
          id: integer() | nil,
          table_id: integer() | nil,
          snapshot: DateTime.t() | nil,
          snapshot_s3_key: String.t() | nil,
          deleted_at: DateTime.t() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  schema "cubic_ods_table_snapshots" do
    field(:table_id, :integer)
    field(:snapshot, :utc_datetime)
    field(:snapshot_s3_key, :string)

    field(:deleted_at, :utc_datetime)

    timestamps(type: :utc_datetime)
  end

  @spec not_deleted :: Ecto.Queryable.t()
  defp not_deleted do
    from(ods_table_snapshot in __MODULE__, where: is_nil(ods_table_snapshot.deleted_at))
  end

  @spec get_by!(Keyword.t() | map(), Keyword.t()) :: t() | nil
  def get_by!(clauses, opts \\ []) do
    Repo.get_by!(not_deleted(), clauses, opts)
  end

  @spec update_snapshot(t(), DateTime.t()) :: t()
  def update_snapshot(rec, snapshot) do
    Repo.update!(Changeset.change(rec, %{snapshot: snapshot}))
  end
end
