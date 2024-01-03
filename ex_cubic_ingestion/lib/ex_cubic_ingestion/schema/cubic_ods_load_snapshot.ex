defmodule ExCubicIngestion.Schema.CubicOdsLoadSnapshot do
  @moduledoc """
  ODS loads requires us to store additional information for the load. In particular
  this snapshot is the table's snapshot value at the time of when the load was created.
  """
  use Ecto.Schema

  import Ecto.Query

  alias ExCubicIngestion.Repo

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
  When we use snapshot in paths, we need to format so it's URL-friendly.
  """
  @spec formatted(DateTime.t()) :: String.t()
  def formatted(snapshot) do
    Calendar.strftime(snapshot, "%Y%m%dT%H%M%SZ")
  end
end
