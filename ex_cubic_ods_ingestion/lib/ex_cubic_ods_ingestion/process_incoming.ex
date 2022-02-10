defmodule ExCubicOdsIngestion.ProcessIncoming do
  @moduledoc """
  ProcessIncoming module.
  """

  use GenServer

  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  require Logger
  require ExAws.S3

  import Ecto.Query

  @wait_interval_ms 5_000

  # client methods
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, [])
  end

  def status(server) do
    GenServer.call(server, :status)
  end

  # callbacks
  @impl true
  def init(_opts) do
    {:ok, %{status: :running}, 0}
  end

  @impl true
  def handle_info(:timeout, %{} = state) do
    new_state = run(state)

    # set timeout according to need for continuing
    timeout =
      if new_state[:continuation_token] == "" do
        @wait_interval_ms
      else
        0
      end

    {:noreply, new_state, timeout}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, state[:status], state}
  end

  # server helper functions
  @spec run(map()) :: map()
  defp run(state) do
    # get list of load objects for vendor
    [load_objects, next_continuation_token] =
      load_objects_list("cubic_ods_qlik/", state[:continuation_token])

    # query loads to see what we can ignore when inserting
    # usually happens when objects have not been moved out of 'incoming' bucket
    [first_load_object | _rest] = load_objects

    load_recs =
      if first_load_object do
        load_recs_list(first_load_object)
      else
        []
      end

    # filter out objects already in the database
    new_load_objects = Enum.filter(load_objects, &filter_already_added(&1, load_recs))

    # insert new load objects
    Repo.transaction(fn -> Enum.each(new_load_objects, &insert_load(&1)) end)

    # add or update continuation_token
    Map.merge(state, %{continuation_token: next_continuation_token})
  end

  @spec load_objects_list(String.t(), String.t()) :: list()
  def load_objects_list(vendor_prefix, continuation_token, max_keys \\ 1_000) do
    # get config variables
    aws = Application.get_env(:ex_cubic_ods_ingestion, :lib_ex_aws, ExAws)
    aws_s3 = Application.get_env(:ex_cubic_ods_ingestion, :lib_ex_aws_s3, ExAws.S3)
    bucket = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_incoming)
    prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_prefix_incoming)

    list_arguments =
      if continuation_token != "" do
        [
          prefix: "#{prefix}#{vendor_prefix}",
          max_keys: max_keys,
          continuation_token: continuation_token
        ]
      else
        [prefix: "#{prefix}#{vendor_prefix}", max_keys: max_keys]
      end

    %{body: %{contents: contents, next_continuation_token: next_continuation_token}} =
      aws_s3.list_objects_v2(bucket, list_arguments) |> aws.request!()

    # @todo handle error cases

    [contents, next_continuation_token]
  end

  @spec load_recs_list(map()) :: list()
  def load_recs_list(load_object) do
    {:ok, last_modified, _offset} = DateTime.from_iso8601(load_object[:last_modified])
    last_modified = DateTime.truncate(last_modified, :second)

    query =
      from(load in CubicOdsLoad,
        where: load.s3_modified >= ^last_modified
      )

    Repo.all(query)
  end

  @spec filter_already_added(map(), list()) :: boolean()
  def filter_already_added(load_object, load_recs) do
    key = load_object[:key]
    {:ok, last_modified, _offset} = DateTime.from_iso8601(load_object[:last_modified])
    last_modified = DateTime.truncate(last_modified, :second)

    not Enum.any?(
      load_recs,
      fn r -> r.s3_key == key and r.s3_modified == last_modified end
    )
  end

  @spec insert_load(map()) :: Ecto.Schema.t()
  def insert_load(new_load_object) do
    {:ok, last_modified, _offset} = DateTime.from_iso8601(new_load_object[:last_modified])
    last_modified = DateTime.truncate(last_modified, :second)
    size = String.to_integer(new_load_object[:size])

    Repo.insert!(%CubicOdsLoad{
      status: "ready",
      s3_key: new_load_object[:key],
      s3_modified: last_modified,
      s3_size: size
    })
  end
end
