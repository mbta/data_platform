defmodule ExCubicIngestion.ProcessIncoming do
  @moduledoc """
  ProcessIncoming server.

  Every @wait_interval_ms, scans the Incoming bucket for table prefixes. If a
  prefix is present here and has a record in CubicTable, the prefix is
  scanned for files, which are inserted as CubicLoad records to be processed
  in the future.
  """

  use GenServer

  alias ExCubicIngestion.S3Scan
  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Validators

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
    incoming_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_incoming)
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    table_prefixes =
      state
      |> prefixes_list(incoming_bucket, incoming_prefix)
      |> Enum.filter(&Map.has_key?(&1, :prefix))
      |> Enum.map(fn %{prefix: prefix} -> String.replace_prefix(prefix, incoming_prefix, "") end)
      |> CubicTable.filter_to_existing_prefixes()

    for {table_prefix, table} <- table_prefixes do
      incoming_bucket
      |> S3Scan.list_objects_v2(
        prefix: "#{incoming_prefix}#{table_prefix}",
        lib_ex_aws: state.lib_ex_aws
      )
      # filter s3 objects to only data objects with a size specified
      |> Enum.filter(&Validators.valid_s3_object?(&1))
      |> Enum.map(fn object ->
        %{object | key: String.replace_prefix(object[:key], incoming_prefix, "")}
      end)
      |> CubicLoad.insert_new_from_objects_with_table(table)
    end

    :ok
  end

  @doc """
  Gets a list of S3 prefixes for each vendor to be used in determining table prefixes.
  """
  @spec prefixes_list(t, String.t(), String.t()) :: Enumerable.t()
  def prefixes_list(state, incoming_bucket, incoming_prefix) do
    ods_qlik_list =
      S3Scan.list_objects_v2(
        incoming_bucket,
        prefix: "#{incoming_prefix}cubic/ods_qlik/",
        delimiter: "/",
        lib_ex_aws: state.lib_ex_aws
      )

    dmap_list =
      S3Scan.list_objects_v2(
        incoming_bucket,
        prefix: "#{incoming_prefix}cubic/dmap/",
        delimiter: "/",
        lib_ex_aws: state.lib_ex_aws
      )

    Enum.concat(ods_qlik_list, dmap_list)
  end
end
