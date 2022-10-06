defmodule ExCubicIngestion.Schema.CubicLoad do
  @moduledoc """
  Contains information on the objects passing through the 'incoming' S3 bucket, as well
  their status while transitioning through the various steps in the data pipeline process.

  Statuses:
    "ready"
    "ingesting"
    "ready_for_archiving"
    "ready_for_erroring"
    "archiving"
    "erroring"
    "archived"
    "archived_unknown"
    "errored"
    "errored_unknown"
  """
  use Ecto.Schema

  import Ecto.Query

  alias Ecto.Changeset
  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable

  @derive {Jason.Encoder,
           only: [
             :id,
             :table_id,
             :status,
             :s3_key,
             :s3_modified,
             :s3_size,
             :is_raw,
             :deleted_at,
             :inserted_at,
             :updated_at
           ]}

  @type t :: %__MODULE__{
          id: integer() | nil,
          table_id: integer(),
          status: String.t() | nil,
          s3_key: String.t() | nil,
          s3_modified: DateTime.t() | nil,
          s3_size: integer() | nil,
          is_raw: boolean() | nil,
          deleted_at: DateTime.t() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  schema "cubic_loads" do
    field(:table_id, :integer)
    field(:status, :string)
    field(:s3_key, :string)
    field(:s3_modified, :utc_datetime)
    field(:s3_size, :integer)
    field(:is_raw, :boolean)

    field(:deleted_at, :utc_datetime)

    timestamps(type: :utc_datetime)
  end

  @spec not_deleted :: Ecto.Queryable.t()
  defp not_deleted do
    from(load in __MODULE__, where: is_nil(load.deleted_at))
  end

  @spec get!(integer()) :: t()
  def get!(id) do
    Repo.get!(not_deleted(), id)
  end

  @doc """
  From a list of S3 objects defined as maps, filter in only new objects and
  insert them in database.
  """
  @spec insert_new_from_objects_with_table([map()], CubicTable.t()) ::
          {:ok, [t()]} | {:error, term()}
  def insert_new_from_objects_with_table(objects, table) do
    # get the last inserted load which will be used to further filter the objects
    last_inserted_load_rec =
      Repo.one(
        from(load in not_deleted(),
          where: load.table_id == ^table.id,
          order_by: [desc: load.s3_modified],
          limit: 1
        )
      )

    # create a list of objects that have a last modified that is later than
    # the last object we have in database
    new_objects =
      Enum.filter(objects, fn object ->
        last_modified = parse_and_drop_msec(object.last_modified)

        is_nil(last_inserted_load_rec) or
          DateTime.compare(last_modified, last_inserted_load_rec.s3_modified) == :gt
      end)

    if Enum.empty?(new_objects) do
      {:ok, []}
    else
      Repo.transaction(fn ->
        # insert new objects
        Enum.map(new_objects, &insert_from_object_with_table(&1, table))
      end)
    end
  end

  @doc """
  Inserts S3 object into database, by doing some pre-processing and adjusting
  status to 'ready_for_erroring' if object size is not greater than 0.
  """
  @spec insert_from_object_with_table(map(), CubicTable.t()) :: t()
  def insert_from_object_with_table(object, table) do
    last_modified = parse_and_drop_msec(object.last_modified)
    size = String.to_integer(object.size)

    status =
      if size > 0 do
        "ready"
      else
        "ready_for_erroring"
      end

    Repo.insert!(%__MODULE__{
      table_id: table.id,
      status: status,
      s3_key: object.key,
      s3_modified: last_modified,
      s3_size: size,
      is_raw: table.is_raw
    })
  end

  @doc """
  Get loads with the 'ready' status by getting all the active tables and
  querying for loads by table.
  """
  @spec get_status_ready :: [t()]
  def get_status_ready do
    # we need to get 'ready' loads only for active tables
    CubicTable.all_with_ods_table_snapshot()
    |> Enum.map(&get_status_ready_for_table_query(&1))
    |> Enum.filter(&(!is_nil(&1)))
    |> Enum.flat_map(&Repo.all(&1))
  end

  @doc """
  Get records by a list of statuses.
  """
  @spec all_by_status_in([String.t()], integer()) :: [t()]
  def all_by_status_in(statuses, limit \\ 1000) do
    query =
      from(load in not_deleted(),
        where: load.status in ^statuses,
        order_by: load.id,
        limit: ^limit
      )

    Repo.all(query)
  end

  @spec get_many_with_table([integer()]) :: [{t(), CubicTable.t()}]
  def get_many_with_table(load_rec_ids) do
    Repo.all(
      from(load in not_deleted(),
        join: table in CubicTable,
        on: table.id == load.table_id,
        where: load.id in ^load_rec_ids,
        select: {load, table}
      )
    )
  end

  @spec change(t(), %{required(atom()) => term()}) :: Changeset.t()
  def change(load_rec, changes) do
    Changeset.change(load_rec, changes)
  end

  # @todo consider making this more specific to use cases
  @spec update(t(), map()) :: t()
  def update(load_rec, changes) do
    {:ok, load_rec} =
      Repo.transaction(fn ->
        Repo.update!(change(load_rec, changes))
      end)

    load_rec
  end

  @spec query_many([integer()]) :: Ecto.Queryable.t()
  def query_many(load_rec_ids) do
    from(load in not_deleted(), where: load.id in ^load_rec_ids, select: load)
  end

  # @todo consider making this more specific to use cases
  @spec update_many([integer()], Keyword.t()) :: [t()]
  def update_many(load_rec_ids, change) do
    {:ok, {_count, updated_load_recs}} =
      Repo.transaction(fn ->
        Repo.update_all(
          query_many(load_rec_ids),
          set: change
        )
      end)

    updated_load_recs
  end

  # private
  @spec parse_and_drop_msec(String.t()) :: DateTime.t()
  defp parse_and_drop_msec(datetime) do
    {:ok, datetime_with_msec, _offset} = DateTime.from_iso8601(datetime)

    DateTime.truncate(datetime_with_msec, :second)
  end

  @doc """
  Construct query for loads with the ready status, filtered by the table and ordered by
  the S3 modified and key values. For tables that are ODS, because there might be a
  Qlik restart between the 'ready' and 'ingesting' status, we should check to make
  sure we only start since the last snapshot load's s3_modified. Because of this
  logic, some 'ready' loads will be left behind. That's because they will be
  pointing to non-existing objects (Note: Qlik removes all objects on a restart).
  Lastly, if no snapshot load exists, just return nil as loads may have not come
  through yet.
  """
  @spec get_status_ready_for_table_query({CubicTable.t(), CubicOdsTableSnapshot.t()}, integer()) ::
          Ecto.Queryable.t() | nil
  def get_status_ready_for_table_query(table_and_ods_table_snapshot, limit \\ 100)

  def get_status_ready_for_table_query({table_rec, nil}, limit) do
    from(load in not_deleted(),
      where: load.status == "ready" and load.table_id == ^table_rec.id,
      order_by: [load.s3_modified, load.s3_key],
      limit: ^limit
    )
  end

  def get_status_ready_for_table_query({table_rec, ods_table_snapshot_rec}, limit) do
    # get the last load with snapshot key
    last_snapshot_load_rec =
      Repo.one(
        from(load in not_deleted(),
          where: load.s3_key == ^ods_table_snapshot_rec.snapshot_s3_key,
          order_by: [desc: load.s3_modified],
          limit: 1
        )
      )

    if not is_nil(last_snapshot_load_rec) do
      from(load in not_deleted(),
        where:
          load.status == "ready" and load.table_id == ^table_rec.id and
            load.s3_modified >= ^last_snapshot_load_rec.s3_modified,
        order_by: [load.s3_modified, load.s3_key],
        limit: ^limit
      )
    end
  end
end
