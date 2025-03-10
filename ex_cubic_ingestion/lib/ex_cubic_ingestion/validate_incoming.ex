defmodule ExCubicIngestion.ValidateIncoming do
  @moduledoc """
  Queries for 'ready' loads and runs validation to make sure we can process
  them further.
  """

  use GenServer

  alias ExCubicIngestion.Schema.CubicLoad

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
  def run(_state) do
    ready_loads = CubicLoad.get_status_ready()

    # start ingestion for those with valid schemas
    Enum.each(ready_loads, fn {load_rec, _table_rec} ->
      CubicLoad.update(load_rec, %{status: "ready_for_ingesting"})
    end)

    :ok
  end
end
