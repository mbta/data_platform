defmodule ExCubicOdsIngestion.ProcessIncoming do
  @moduledoc """
  ProcessIncoming server.

  Every @wait_interval_ms, scans the Incoming bucket for table prefixes. If a
  prefix is present here and has a record in CubicOdsTable, the prefix is
  scanned for files, which are inserted as CubicOdsLoad records to be processed
  in the future.
  """

  use GenServer

  alias ExCubicOdsIngestion.S3Scan
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Schema.CubicOdsTable

  @wait_interval_ms 5_000

  @opaque t :: %__MODULE__{lib_ex_aws: module()}
  defstruct lib_ex_aws: ExAws

  # client methods
  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
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

    {:ok, state, 0}
  end

  @impl GenServer
  def handle_info(:timeout, %{} = state) do
    run(state)

    {:noreply, state, @wait_interval_ms}
  end

  @impl GenServer
  def handle_call(:status, _from, state) do
    {:reply, :running, state}
  end

  # server helper functions
  @spec run(t) :: :ok
  def run(state) do
    incoming_bucket = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_incoming)
    incoming_prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_prefix_incoming)

    table_prefixes =
      incoming_bucket
      |> S3Scan.list_objects_v2(
        prefix: "#{incoming_prefix}cubic_ods_qlik/",
        delimiter: "/",
        lib_ex_aws: state.lib_ex_aws
      )
      |> Stream.filter(&Map.has_key?(&1, :prefix))
      |> Enum.map(&Map.fetch!(&1, :prefix))
      |> CubicOdsTable.filter_to_existing_prefixes()

    for {table_prefix, table} <- table_prefixes do
      incoming_bucket
      |> S3Scan.list_objects_v2(
        prefix: "#{incoming_prefix}#{table_prefix}",
        lib_ex_aws: state.lib_ex_aws
      )
      |> Enum.filter(&Map.has_key?(&1, :key))
      |> CubicOdsLoad.insert_new_from_objects_with_table(table)
    end

    :ok
  end
end
