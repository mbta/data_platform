defmodule ExCubicIngestion.Schema.CubicLoad do
  @moduledoc """
  Contains information on the objects passing through the 'incoming' S3 bucket, as well
  their status while transitioning through the various steps in the data pipeline process.

  Statuses:
    "archived"
    "archived_unknown"
    "archiving"
    "errored"
    "errored_unknown"
    "erroring"
    "ingesting"
    "ready"
    "ready_for_archiving"
    "ready_for_erroring"
    "ready_for_ingesting"

  Status Flow Chart:
  https://miro.com/app/board/o9J_liWCxTw=/?moveToWidget=3458764538953053054&cot=14
  """
  use Ecto.Schema

  import Ecto.Query

  alias Ecto.Changeset
  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot
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

  @spec process_objects(map(), CubicTable.t(), {CubicOdsTableSnapshot.t() | nil, []}) ::
          {CubicOdsTableSnapshot.t() | nil,
           [
             {t(), CubicOdsLoadSnapshot.t() | nil, CubicTable.t(),
              CubicOdsTableSnapshot.t() | nil}
           ]}
  defp process_objects(object, table, {latest_ods_table_snapshot, latest_inserted_loads}) do
    inserted_load =
      {_load, _ods_load_snapshot, _table, updated_ods_table_snapshot} =
      insert_from_object_with_table(object, {table, latest_ods_table_snapshot})

    {updated_ods_table_snapshot, [inserted_load | latest_inserted_loads]}
  end

  @doc """
  From a list of S3 objects defined as maps, filter in only new objects since last load and
  insert them in database.
  """
  @spec insert_new_from_objects_with_table([map()], CubicTable.t()) ::
          {:ok,
           {CubicOdsTableSnapshot.t() | nil,
            [
              {t(), CubicOdsLoadSnapshot.t() | nil, CubicTable.t(),
               CubicOdsTableSnapshot.t() | nil}
            ]}}
          | {:error, term()}
  def insert_new_from_objects_with_table(objects, table) do
    # get ODS snapshot if available
    ods_table_snapshot = CubicOdsTableSnapshot.get_by(table_id: table.id)

    # get the last inserted load
    last_inserted_load =
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

        is_nil(last_inserted_load) or
          DateTime.compare(last_modified, last_inserted_load.s3_modified) == :gt
      end)

    if Enum.empty?(new_objects) do
      {:ok, {ods_table_snapshot, []}}
    else
      Repo.transaction(fn ->
        Enum.reduce(new_objects, {ods_table_snapshot, []}, &process_objects(&1, table, &2))
      end)
    end
  end

  @doc """
  Insert load record from an S3 object. For ODS, we also insert an ods_load_snapshot record
  and update ods_table_snapshot record if we the S3 object correspond to a snapshot key.
  """
  @spec insert_from_object_with_table(map(), {CubicTable.t(), CubicOdsTableSnapshot.t() | nil}) ::
          {t(), CubicOdsLoadSnapshot.t() | nil, CubicTable.t(), CubicOdsTableSnapshot.t() | nil}
  def insert_from_object_with_table(object, {table, nil}) do
    last_modified = parse_and_drop_msec(object.last_modified)
    size = String.to_integer(object.size)

    status =
      if size > 0 do
        "ready"
      else
        "ready_for_erroring"
      end

    load =
      Repo.insert!(%__MODULE__{
        table_id: table.id,
        status: status,
        s3_key: object.key,
        s3_modified: last_modified,
        s3_size: size,
        is_raw: table.is_raw
      })

    {load, nil, table, nil}
  end

  # for ODS we need to do some extra work
  def insert_from_object_with_table(object, {table, ods_table_snapshot}) do
    last_modified = parse_and_drop_msec(object.last_modified)
    size = String.to_integer(object.size)

    # first update the snapshot, if we have encountered an S3 key that matches
    updated_ods_table_snapshot =
      if ods_table_snapshot.snapshot_s3_key == object.key do
        CubicOdsTableSnapshot.update_snapshot(ods_table_snapshot, last_modified)
      else
        ods_table_snapshot
      end

    # only ready if we have a snapshot
    status =
      if size > 0 and not is_nil(updated_ods_table_snapshot.snapshot) do
        "ready"
      else
        "ready_for_erroring"
      end

    load =
      Repo.insert!(%__MODULE__{
        table_id: table.id,
        status: status,
        s3_key: object.key,
        s3_modified: last_modified,
        s3_size: size,
        is_raw: table.is_raw
      })

    # insert a snapshot record for the load
    ods_load_snapshot =
      Repo.insert!(%CubicOdsLoadSnapshot{
        load_id: load.id,
        snapshot: updated_ods_table_snapshot.snapshot
      })

    {load, ods_load_snapshot, table, updated_ods_table_snapshot}
  end

  @doc """
  Get loads with the 'ready' status by getting all the active tables and
  querying for loads by table.
  """
  @spec get_status_ready :: [{t(), CubicTable.t()}]
  def get_status_ready do
    # we need to get 'ready' loads only for active tables
    CubicTable.all_with_ods_table_snapshot()
    |> Enum.map(&get_status_ready_by_table_query(&1))
    |> Enum.filter(&(!is_nil(&1)))
    |> Enum.flat_map(fn {table, ready_loads_by_table_query} ->
      Enum.map(Repo.all(ready_loads_by_table_query), fn ready_load ->
        {ready_load, table}
      end)
    end)
  end

  @doc """
  Get records by a list of statuses.
  """
  @spec all_by_status_in([String.t()], integer()) :: [t()]
  def all_by_status_in(statuses, limit \\ 1000) do
    query =
      from(load in not_deleted(),
        where: load.status in ^statuses,
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
  @spec get_status_ready_by_table_query({CubicTable.t(), CubicOdsTableSnapshot.t()}, integer()) ::
          {CubicTable.t(), Ecto.Queryable.t()} | nil
  def get_status_ready_by_table_query(table_and_ods_table_snapshot, limit \\ 100)

  def get_status_ready_by_table_query({table_rec, nil}, limit) do
    {table_rec,
     from(load in not_deleted(),
       where: load.status == "ready" and load.table_id == ^table_rec.id,
       limit: ^limit
     )}
  end

  def get_status_ready_by_table_query({table_rec, ods_table_snapshot_rec}, limit) do
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
      {table_rec,
       from(load in not_deleted(),
         where:
           load.status == "ready" and load.table_id == ^table_rec.id and
             load.s3_modified >= ^last_snapshot_load_rec.s3_modified,
         limit: ^limit
       )}
    end
  end

  @spec ods_load?(String.t()) :: boolean()
  @doc """
  Check the S3 key provided starts with the path where ODS loads are uploaded.
  """
  def ods_load?(s3_key) do
    String.starts_with?(s3_key, "cubic/ods_qlik/")
  end

  @spec parse_and_drop_msec(String.t()) :: DateTime.t()
  def parse_and_drop_msec(datetime) do
    {:ok, datetime_with_msec, _offset} = DateTime.from_iso8601(datetime)

    DateTime.truncate(datetime_with_msec, :second)
  end

  @spec glue_job_payload({t(), CubicTable.t()}) :: map()
  @doc """
  Using Cubic load and table information, return the payload the Glue job will need.
  """
  def glue_job_payload({load_rec, table_rec}) do
    bucket_incoming = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_incoming)
    bucket_springboard = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_springboard)

    prefix_incoming = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    prefix_springboard =
      Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_springboard)

    destination_path =
      if load_rec.is_raw do
        "raw/#{Path.dirname(load_rec.s3_key)}"
      else
        Path.dirname(load_rec.s3_key)
      end

    destination_path_ct = String.ends_with?(destination_path, "__ct")

    source_table_name =
      if destination_path_ct do
        "#{table_rec.name}__ct"
      else
        table_rec.name
      end

    destination_table_name =
      cond do
        load_rec.is_raw && destination_path_ct ->
          "raw_#{table_rec.name}__ct"

        load_rec.is_raw ->
          "raw_#{table_rec.name}"

        destination_path_ct ->
          "#{table_rec.name}__ct"

        true ->
          table_rec.name
      end

    %{
      id: load_rec.id,
      s3_key: load_rec.s3_key,
      source_table_name: source_table_name,
      destination_table_name: destination_table_name,
      source_s3_key: "s3://#{bucket_incoming}/#{prefix_incoming}#{load_rec.s3_key}",
      # note: 's3a' is intentional and is the protocol glue/spark uses to write to S3
      destination_path: "s3a://#{bucket_springboard}/#{prefix_springboard}#{destination_path}",
      partition_columns: [
        %{name: "identifier", value: Path.basename(load_rec.s3_key)}
      ]
    }
  end
end
