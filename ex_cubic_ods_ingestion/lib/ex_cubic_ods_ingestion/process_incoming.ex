defmodule ExCubicOdsIngestion.ProcessIncoming do
  @moduledoc """
  ProcessIncoming module.
  """

  use GenServer

  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  require ExAws
  require ExAws.S3

  @wait_interval_ms 5_000

  defstruct [:lib_ex_aws, status: :not_started, continuation_token: "", max_keys: 1_000]

  # client methods
  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    # define lib_ex_aws, unless it's already defined
    opts = Keyword.put_new(opts, :lib_ex_aws, ExAws)

    GenServer.start_link(__MODULE__, opts)
  end

  @spec status(GenServer.server()) :: :running
  def status(server) do
    GenServer.call(server, :status)
  end

  # callbacks
  @impl GenServer
  def init(opts) do
    # construct state
    state = struct!(__MODULE__, opts)

    {:ok, %{state | status: :running}, 0}
  end

  @impl GenServer
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

  @impl GenServer
  def handle_call(:status, _from, state) do
    {:reply, state.status, state}
  end

  # server helper functions
  @spec run(map()) :: map()
  defp run(state) do
    # get list of load objects for vendor
    {load_objects, next_continuation_token} = load_objects_list("cubic_ods_qlik/", state)

    # insert new load objects
    CubicOdsLoad.insert_new_from_objects(load_objects)

    # update state
    %{state | continuation_token: next_continuation_token}
  end

  @spec load_objects_list(String.t(), struct()) :: {list(), String.t()}
  def load_objects_list(vendor_prefix, state) do
    # get config variables
    bucket = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_incoming)
    prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_prefix_incoming)

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
      state.lib_ex_aws.request!(ExAws.S3.list_objects_v2(bucket, list_arguments))

    # @todo handle error cases

    {contents, next_continuation_token}
  end
end
