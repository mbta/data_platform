defmodule ExCubicIngestion.ProcessIngestion do
  @moduledoc """
  ProcessIngestion module.
  """

  use GenServer

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Workers.Archive
  alias ExCubicIngestion.Workers.Error
  alias ExCubicIngestion.Workers.Ingest

  @wait_interval_ms 60_000
  # maxes for each chunk
  @max_num_of_loads 10
  @max_size_of_loads 100_000_000

  defstruct status: :not_started

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
    ["ready_for_archiving", "ready_for_erroring", "ready_for_ingesting"]
    |> CubicLoad.all_by_status_in()
    |> process_loads()

    # return
    state
  end

  @doc """
  Create different jobs for the different status. The 'ready_for_ingesting' loads will also be
  chunked up, so we can initiate one Glue job for multiple loads.
  """
  @spec process_loads([CubicLoad.t()]) :: :ok
  def process_loads(load_recs) do
    {ingest_loads, archive_error_loads} =
      Enum.split_with(load_recs, fn load_rec -> load_rec.status == "ready_for_ingesting" end)

    ingest_loads
    |> chunk_loads(@max_num_of_loads, @max_size_of_loads)
    |> Enum.map(&Enum.map(&1, fn load_rec -> load_rec.id end))
    |> Enum.each(&ingest/1)

    Enum.each(archive_error_loads, fn load_rec ->
      # ready_for_erroring
      if load_rec.status == "ready_for_archiving" do
        archive(load_rec)
      else
        error(load_rec)
      end
    end)

    :ok
  end

  @doc """
  Chunks the loads up by a size and number maximum, allowing for more efficient job allocation
  """
  @spec chunk_loads([CubicLoad.t()], integer(), integer()) :: [[CubicLoad.t()]]
  def chunk_loads(loads, max_num_of_loads, max_size_of_loads) do
    chunk_fun = fn element, acc ->
      total_acc_s3_size =
        Enum.reduce(acc, 0, fn element, acc_s3_size ->
          element.s3_size + acc_s3_size
        end)

      cond do
        length(acc) == max_num_of_loads ->
          {:cont, Enum.reverse(acc), [element]}

        total_acc_s3_size + element.s3_size > max_size_of_loads ->
          {:cont, Enum.reverse(acc), [element]}

        true ->
          {:cont, [element | acc]}
      end
    end

    after_fun = fn
      [] -> {:cont, []}
      acc -> {:cont, Enum.reverse(acc), []}
    end

    Enum.chunk_while(loads, [], chunk_fun, after_fun)
  end

  @doc """
  For each load that's about to be archived, updated its status and insert an Archive job.
  """
  @spec archive(CubicLoad.t()) :: {atom(), map()}
  def archive(load_rec) do
    Ecto.Multi.new()
    |> Ecto.Multi.update(:update_status, CubicLoad.change(load_rec, %{status: "archiving"}))
    |> Oban.insert(:archive_job, Archive.new(%{load_rec_id: load_rec.id}))
    |> Repo.transaction()
  end

  @doc """
  For each load that's about to be errored out, updated its status and insert an Error job.
  """
  @spec error(CubicLoad.t()) :: {atom(), map()}
  def error(load_rec) do
    Ecto.Multi.new()
    |> Ecto.Multi.update(:update_status, CubicLoad.change(load_rec, %{status: "erroring"}))
    |> Oban.insert(:error_job, Error.new(%{load_rec_id: load_rec.id}))
    |> Repo.transaction()
  end

  @doc """
  For a batch of loads we want ingest, updated their status and insert one Ingest job.
  """
  @spec ingest([integer()]) :: {atom(), map()}
  def ingest(load_rec_ids) do
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
