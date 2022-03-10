defmodule ExCubicOdsIngestion.StartIngestion do
  @moduledoc """
  StartIngestion module.
  """

  use GenServer

  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Schema.CubicOdsTable
  alias ExCubicOdsIngestion.Workers.Ingest

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
    CubicOdsLoad.get_status_ready()
    |> Enum.map(&update_snapshot/1)
    |> chunk_loads()
    |> Enum.each(&process_loads/1)
  end

  @spec process_loads([CubicOdsLoad.t(), ...]) ::
          :ok
  def process_loads([_ | _] = ready_load_chunk) do
    start_ingestion(Enum.map(ready_load_chunk, & &1.id))

    :ok
  end

  @spec update_snapshot(CubicOdsLoad.t()) :: CubicOdsLoad.t()
  def update_snapshot(load_rec) do
    # find the table rec that the load is for
    table_rec = CubicOdsTable.get!(load_rec.table_id)

    # for table, update snapshot if we have a new snapshot, as in
    # the load key matches the snapshot key and the load modified date is newer than the snapshot
    updated_table_rec =
      if table_rec.snapshot_s3_key == load_rec.s3_key and
           (is_nil(table_rec.snapshot) or
              DateTime.compare(table_rec.snapshot, load_rec.s3_modified) == :lt) do
        CubicOdsTable.update(table_rec, %{snapshot: load_rec.s3_modified})
      else
        table_rec
      end

    if is_nil(load_rec.snapshot) do
      CubicOdsLoad.update(load_rec, %{snapshot: updated_table_rec.snapshot})
    else
      load_rec
    end
  end

  @spec chunk_loads([CubicOdsLoad.t()]) :: [[CubicOdsLoad.t(), ...]]
  defp chunk_loads(loads) do
    # @todo replace chunk_every with chunk_while for more fine-tuned control
    Enum.chunk_every(loads, 3)
  end

  @spec start_ingestion([integer()]) :: {atom(), map()}
  defp start_ingestion(load_rec_ids) do
    Ecto.Multi.new()
    |> Ecto.Multi.update_all(
      :update_status,
      CubicOdsLoad.query_many(load_rec_ids),
      set: [status: "ingesting"]
    )
    |> Oban.insert(:ingest_job, Ingest.new(%{load_rec_ids: load_rec_ids}))
    |> Repo.transaction()
  end
end
