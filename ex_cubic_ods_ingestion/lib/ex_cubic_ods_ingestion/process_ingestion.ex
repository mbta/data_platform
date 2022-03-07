defmodule ExCubicOdsIngestion.ProcessIngestion do
  @moduledoc """
  ProcessIngestion module.
  """

  use GenServer

  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Workers.Archive
  alias ExCubicOdsIngestion.Workers.Error

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
    new_state = run(state)

    # set timeout according to need for continuing
    timeout = @wait_interval_ms

    {:noreply, new_state, timeout}
  end

  @impl GenServer
  def handle_call(:status, _from, state) do
    {:reply, state.status, state}
  end

  # server helper functions
  @spec run(map()) :: map()
  defp run(state) do
    # get list of load records that are ready for archive/error, and process them
    process_loads(CubicOdsLoad.get_status_ready_for())

    # return
    state
  end

  @spec process_loads([CubicOdsLoad.t()]) :: :ok
  def process_loads(load_recs) do
    {archive_loads, error_loads} =
      Enum.split_with(load_recs, fn load_rec -> load_rec.status == "ready_for_archiving" end)

    Enum.each(archive_loads, &archive(&1))

    Enum.each(error_loads, &error(&1))

    :ok
  end

  @spec archive(CubicOdsLoad.t()) :: {atom(), map()}
  def archive(load_rec) do
    Ecto.Multi.new()
    |> Ecto.Multi.update(:update_status, CubicOdsLoad.change(load_rec, %{status: "archiving"}))
    |> Oban.insert(:archive_job, Archive.new(%{load_rec_id: load_rec.id}))
    |> Repo.transaction()
  end

  @spec error(CubicOdsLoad.t()) :: {atom(), map()}
  def error(load_rec) do
    Ecto.Multi.new()
    |> Ecto.Multi.update(:update_status, CubicOdsLoad.change(load_rec, %{status: "erroring"}))
    |> Oban.insert(:error_job, Error.new(%{load_rec_id: load_rec.id}))
    |> Repo.transaction()
  end
end
