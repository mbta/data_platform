defmodule ExCubicIngestion.Schema.CubicOdsLoadSnapshot do
  @moduledoc """
  ODS loads requires us to store additional information for the load. In particular
  this snapshot is the table's snapshot value at the time of when the load was created.
  """
  use Ecto.Schema

  import Ecto.Query

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot

  @derive {Jason.Encoder,
           only: [
             :id,
             :load_id,
             :snapshot,
             :deleted_at,
             :inserted_at,
             :updated_at
           ]}

  @type t :: %__MODULE__{
          id: integer() | nil,
          load_id: integer() | nil,
          snapshot: DateTime.t() | nil,
          deleted_at: DateTime.t() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  schema "cubic_ods_load_snapshots" do
    field(:load_id, :integer)
    field(:snapshot, :utc_datetime)

    field(:deleted_at, :utc_datetime)

    timestamps(type: :utc_datetime)
  end

  @spec not_deleted :: Ecto.Queryable.t()
  defp not_deleted do
    from(ods_load_snapshot in __MODULE__, where: is_nil(ods_load_snapshot.deleted_at))
  end

  @spec get_by!(Keyword.t() | map(), Keyword.t()) :: t() | nil
  def get_by!(clauses, opts \\ []) do
    Repo.get_by!(not_deleted(), clauses, opts)
  end

  @spec get_latest_by!(Keyword.t() | map()) :: t() | nil
  def get_latest_by!(clauses) do
    Repo.one!(
      from(ods_load_snapshot in not_deleted(),
        where: ^clauses,
        order_by: [desc: ods_load_snapshot.inserted_at],
        limit: 1
      )
    )
  end

  @doc """
  Updates snapshots for ODS table (if needed), and inserts new ODS load snapshot.
  """
  @spec update_snapshot(CubicLoad.t()) :: :ok
  def update_snapshot(load_rec) do
    # will raise error if no results found
    ods_table_snapshot_rec = CubicOdsTableSnapshot.get_by!(table_id: load_rec.table_id)

    # update ODS table snapshot if we have a new snapshot, as in the load key
    # matches the snapshot key and the load modified date is newer than the snapshot
    updated_ods_table_snapshot_rec =
      if ods_table_snapshot_rec.snapshot_s3_key == load_rec.s3_key and
           (is_nil(ods_table_snapshot_rec.snapshot) or
              DateTime.compare(ods_table_snapshot_rec.snapshot, load_rec.s3_modified) == :lt) do
        CubicOdsTableSnapshot.update_snapshot(ods_table_snapshot_rec, load_rec.s3_modified)
      else
        ods_table_snapshot_rec
      end

    Repo.insert!(%__MODULE__{
      load_id: load_rec.id,
      snapshot: updated_ods_table_snapshot_rec.snapshot
    })

    :ok
  end
end
