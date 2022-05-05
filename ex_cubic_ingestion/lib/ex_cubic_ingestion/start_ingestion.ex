defmodule ExCubicIngestion.StartIngestion do
  @moduledoc """
  StartIngestion module.
  """

  use GenServer

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Workers.Ingest

  require Oban
  require Oban.Job

  @wait_interval_ms 5_000

  defstruct status: :not_started, continuation_token: "", max_keys: 1_000

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

    {:ok, %{state | status: :running}, 0}
  end

  @impl GenServer
  def handle_info(:timeout, %{} = state) do
    run()

    # set timeout according to need for continuing
    timeout = @wait_interval_ms

    {:noreply, state, timeout}
  end

  @impl GenServer
  def handle_call(:status, _from, state) do
    {:reply, state.status, state}
  end

  # server helper functions
  @spec run() :: :ok
  def run do
    # get list of load records that are in 'ready' state, ordered by s3_modified, s3_key
    # prepare them for processing, and kick off separate flows
    CubicLoad.get_status_ready()
    |> chunk_loads()
    |> Enum.each(&process_loads/1)
  end

  @spec process_loads([CubicLoad.t(), ...]) ::
          :ok
  def process_loads([_ | _] = ready_load_chunk) do
    start_ingestion(Enum.map(ready_load_chunk, & &1.id))

    :ok
  end

  @spec chunk_loads([CubicLoad.t()]) :: [[CubicLoad.t(), ...]]
  defp chunk_loads(loads) do
    # @todo replace chunk_every with chunk_while for more fine-tuned control
    Enum.chunk_every(loads, 3)
  end

  @spec start_ingestion([integer()]) :: {atom(), map()}
  defp start_ingestion(load_rec_ids) do
    Ecto.Multi.new()
    |> Ecto.Multi.update_all(
      :update_status,
      CubicLoad.query_many(load_rec_ids),
      set: [status: "ingesting"]
    )
    |> Oban.insert(:ingest_job, Ingest.new(%{load_rec_ids: load_rec_ids}))
    |> Repo.transaction()
  end
end
