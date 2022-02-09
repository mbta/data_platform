defmodule ExCubicOdsIngestion.ProcessIncoming do
  @moduledoc """
  ProcessIncoming module.
  """

  use GenServer

  alias ExCubicOdsIngestion.Repo

  require Logger
  require ExAws.S3

  import Ecto.Query

  @wait_interval_ms 5_000
  @s3_list_continuation_token ""

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
    timeout = if new_state[:continuation_token] do
      0
    else
      @wait_interval_ms
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
    [ load_objects, next_continuation_token ] =
      load_objects_list("cubic_ods_qlik/", state[:continuation_token])

    # query loads to see what we can ignore when inserting
    # usually happens when objects have not been moved out of 'incoming' bucket
    [first | rest] = load_objects
    load_recs = if first do
      load_recs_list(first)
    else
      []
    end

    # insert load objects as records
    # Enum.map(load_objects, fn object -> Logger.info(object[:key]) end)

    # add or update continuation_token
    Map.merge(state, %{continuation_token: next_continuation_token})

  end

  @spec load_objects_list(String.t(), String.t()) :: list()
  def load_objects_list(vendor_prefix, continuation_token) do
    # fetch config variables
    bucket = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_incoming)
    prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_prefix_incoming)

    arguments = if continuation_token != "" do
      [prefix: "#{prefix}#{vendor_prefix}", continuation_token: continuation_token]
    else
      [prefix: "#{prefix}#{vendor_prefix}"]
    end

    %{body: %{contents: contents, next_continuation_token: next_continuation_token} } =
      ExAws.S3.list_objects_v2(bucket, arguments) |> ExAws.request!()

    # %{body: %{contents: contents, next_continuation_token: next_continuation_token} } =
    #   ExAws.S3.list_objects_v2(
    #     bucket,
    #     prefix: "#{prefix}#{vendor_prefix}",
    #     continuation_token: continuation_token) |> ExAws.request!()

    [contents, next_continuation_token]
  end

  @spec load_recs_list(map()) :: list()
  def load_recs_list(load_object) do
    key = load_object[:key]
    {:ok, last_modified, _offset} = DateTime.from_iso8601(load_object[:last_modified])

    query =
      from(loads in "cubic_ods_loads",
        select: %{s3_key: loads.s3_key},
        where: loads.s3_key >= ^key and loads.s3_modified >= ^last_modified
      )

    Repo.all(query)
  end


end
