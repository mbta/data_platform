defmodule ExCubicOdsIngestion.ProcessIncoming do
  @moduledoc """
  ProcessIncoming module.
  """

  use GenServer

  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  require ExAws
  require ExAws.S3

  import Ecto.Query

  @wait_interval_ms 5_000

  defstruct [:lib_ex_aws, :status, continuation_token: "", max_keys: 1_000]

  # client methods
  def start_link(opts) do
    # define lib_ex_aws, unless it's already defined
    opts = opts |> Keyword.put_new(:lib_ex_aws, ExAws)

    GenServer.start_link(__MODULE__, opts)
  end

  def status(server) do
    GenServer.call(server, :status)
  end

  # callbacks
  @impl true
  def init(opts) do
    # construct state
    state = struct!(__MODULE__, opts)

    {:ok, %{state | status: :running}, 0}
  end

  @impl true
  def handle_info(:timeout, %{} = state) do
    new_state = run(state)

    # set timeout according to need for continuing
    timeout =
      if new_state.continuation_token == "" do
        @wait_interval_ms
      else
        0
      end

    {:noreply, new_state, timeout}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, state.status, state}
  end

  # server helper functions
  @spec run(map()) :: map()
  defp run(state) do
    # get list of load objects for vendor
    [load_objects, next_continuation_token] =
      load_objects_list("cubic_ods_qlik/", state)

    # query loads to see what we can ignore when inserting
    # usually happens when objects have not been moved out of 'incoming' bucket
    load_recs = load_recs_list(List.first(load_objects))

    # create a list of objects that have not been added to database
    new_load_objects = Enum.filter(load_objects, &not_added(&1, load_recs))

    # insert new load objects
    CubicOdsLoad.insert_from_objects(new_load_objects)

    # update state
    %{state | continuation_token: next_continuation_token}
  end

  @spec load_objects_list(String.t(), struct()) :: list()
  def load_objects_list(vendor_prefix, state) do
    # get config variables
    bucket = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_incoming)
    prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_prefix_incoming)

    list_arguments =
      if state.continuation_token != "" do
        [
          prefix: "#{prefix}#{vendor_prefix}",
          max_keys: state.max_keys,
          continuation_token: state.continuation_token
        ]
      else
        [prefix: "#{prefix}#{vendor_prefix}", max_keys: state.max_keys]
      end

    %{body: %{contents: contents, next_continuation_token: next_continuation_token}} =
      ExAws.S3.list_objects_v2(bucket, list_arguments) |> state.lib_ex_aws.request!()

    # @todo handle error cases

    [contents, next_continuation_token]
  end

  @spec load_recs_list(map()) :: list()
  def load_recs_list(load_object) do
    if load_object do
      {:ok, last_modified, _offset} = DateTime.from_iso8601(load_object[:last_modified])
      last_modified = DateTime.truncate(last_modified, :second)

      query =
        from(load in CubicOdsLoad,
          where: load.s3_modified >= ^last_modified
        )

      Repo.all(query)
    else
      []
    end
  end

  @spec not_added(map(), list()) :: boolean()
  def not_added(load_object, load_recs) do
    key = load_object[:key]
    {:ok, last_modified, _offset} = DateTime.from_iso8601(load_object[:last_modified])
    last_modified = DateTime.truncate(last_modified, :second)

    not Enum.any?(
      load_recs,
      fn r -> r.s3_key == key and r.s3_modified == last_modified end
    )
  end
end
