defmodule ExCubicIngestion.ValidateIncoming do
  @moduledoc """
  Queries for 'ready' loads and runs validation to make sure we can process
  them further.
  """

  use GenServer

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.SchemaFetch

  require Logger
  require Oban
  require Oban.Job

  @wait_interval_ms 60_000

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

  @doc """
  Get list of load records that are in 'ready' state, ordered by s3_modified, check
  validity of schema, and set status accordingly for further processing.
  """
  @spec run(t) :: :ok
  def run(state) do
    ready_loads = CubicLoad.get_status_ready()

    # check schemas, and split into valid and invalid loads
    {valid_ready_loads, invalid_ready_loads} =
      Enum.split_with(ready_loads, &valid_schema?(state, &1))

    # start ingestion for those with valid schemas
    Enum.each(valid_ready_loads, fn {load_rec, _table_rec} ->
      CubicLoad.update(load_rec, %{status: "ready_for_ingesting"})
    end)

    # log and error out invalid ones
    Enum.each(invalid_ready_loads, fn {load_rec, _table_rec} = ready_load ->
      Logger.error(
        "[ex_cubic_ingestion] [start_ingestion] Invalid schema detected: #{inspect(ready_load)}"
      )

      CubicLoad.update(load_rec, %{status: "ready_for_erroring"})
    end)

    :ok
  end

  @spec valid_schema?(t(), {CubicLoad.t(), CubicTable.t()}) :: boolean()
  defp valid_schema?(state, {load_rec, table_rec}) do
    # if ODS, check the schema provided by Cubic (.dfm files) against Glue
    if CubicLoad.ods_load?(load_rec.s3_key) do
      SchemaFetch.get_cubic_ods_qlik_columns(state.lib_ex_aws, load_rec) ==
        SchemaFetch.get_glue_columns(state.lib_ex_aws, table_rec, load_rec)
    else
      # @todo implement DMAP schema checker once we have the schema API from Cubic
      true
    end
  end
end
