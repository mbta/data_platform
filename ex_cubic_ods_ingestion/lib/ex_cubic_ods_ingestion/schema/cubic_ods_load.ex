defmodule ExCubicOdsIngestion.Schema.CubicOdsLoad do
  @moduledoc """
  Schema.CubicOdsLoad @todo
  """
  use Ecto.Schema

  import Ecto.Query

  alias Ecto.Changeset
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsTable

  @derive {Jason.Encoder,
           only: [
             :id,
             :table_id,
             :status,
             :snapshot,
             :s3_key,
             :s3_modified,
             :s3_size,
             :deleted_at,
             :inserted_at,
             :updated_at
           ]}

  @type t :: %__MODULE__{
          id: integer() | nil,
          table_id: integer(),
          status: String.t() | nil,
          snapshot: DateTime.t() | nil,
          s3_key: String.t() | nil,
          s3_modified: DateTime.t() | nil,
          s3_size: integer() | nil,
          deleted_at: DateTime.t() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  schema "cubic_ods_loads" do
    field(:table_id, :integer)
    # @todo specify the different statuses
    field(:status, :string)
    field(:snapshot, :utc_datetime)
    field(:s3_key, :string)
    field(:s3_modified, :utc_datetime)
    field(:s3_size, :integer)

    field(:deleted_at, :utc_datetime)

    timestamps(type: :utc_datetime)
  end

  @spec insert_new_from_objects_with_table([map()], CubicOdsTable.t()) ::
          {:ok, [t()]} | {:error, term()}
  def insert_new_from_objects_with_table(objects, table) do
    Repo.transaction(fn ->
      # query loads to see what we can ignore when inserting
      # usually happens when objects have not been moved out of 'incoming' bucket
      recs = get_by_objects(objects)

      # create a list of objects that have not been added to database
      new_objects = Enum.filter(objects, &not_added(&1, recs))

      # insert new objects
      Enum.map(new_objects, &insert_from_object_with_table(&1, table))
    end)
  end

  @spec insert_from_object_with_table(map(), CubicOdsTable.t()) :: Ecto.Schema.t()
  def insert_from_object_with_table(object, table) do
    last_modified = parse_and_drop_msec(object[:last_modified])
    size = String.to_integer(object[:size])

    Repo.insert!(%__MODULE__{
      table_id: table.id,
      status: "ready",
      s3_key: object[:key],
      s3_modified: last_modified,
      s3_size: size
    })
  end

  @spec get_by_objects(list()) :: [t()]
  def get_by_objects(objects) do
    # put together filters based on the object info
    filters =
      Enum.map(objects, fn object ->
        last_modified = parse_and_drop_msec(object[:last_modified])

        {object[:key], last_modified}
      end)

    # we only want to query if we have filters because otherwise the query will the return
    # the whole table
    if Enum.empty?(filters) do
      []
    else
      query_with_filters =
        Enum.reduce(filters, __MODULE__, fn {s3_key, s3_modified}, query ->
          # query
          from(load in query,
            or_where: load.s3_key == ^s3_key and load.s3_modified == ^s3_modified
          )
        end)

      Repo.all(query_with_filters)
    end
  end

  @spec not_added(map(), list()) :: boolean()
  def not_added(load_object, load_recs) do
    key = load_object[:key]
    last_modified = parse_and_drop_msec(load_object[:last_modified])

    not Enum.any?(
      load_recs,
      fn r -> r.s3_key == key and r.s3_modified == last_modified end
    )
  end

  @spec get!(integer()) :: t()
  def get!(id) do
    Repo.get!(__MODULE__, id)
  end

  @spec get_status_ready :: [t()]
  def get_status_ready do
    # @todo add deleted filter
    query =
      from(load in __MODULE__,
        where: load.status == "ready",
        order_by: [load.s3_modified, load.s3_key]
      )

    Repo.all(query)
  end

  @spec get_status_ready_for :: [t()]
  def get_status_ready_for do
    # @todo add deleted filter
    query =
      from(load in __MODULE__,
        where: load.status in ["ready_for_archiving", "ready_for_erroring"]
      )

    Repo.all(query)
  end

  @spec get_many_with_table([integer()]) :: [{t(), CubicOdsTable.t()}]
  def get_many_with_table(load_rec_ids) do
    Repo.all(
      from(load in __MODULE__,
        join: table in CubicOdsTable,
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
    from(load in __MODULE__, where: load.id in ^load_rec_ids, select: load)
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
end
