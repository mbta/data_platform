defmodule ExCubicIngestion.StartIngestion do
  @moduledoc """
  StartIngestion module.
  """

  use GenServer

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot
  alias ExCubicIngestion.Workers.Ingest

  require Oban
  require Oban.Job

  @wait_interval_ms 5_000
  # maxes for each chunk
  @max_num_of_loads 10
  @max_size_of_loads 1000

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

  @doc """
  Get list of load records that are in 'ready' state, ordered by s3_modified, s3_key,
  and prepare them for processing.
  """
  @spec run() :: :ok
  def run do
    ready_loads = CubicLoad.get_status_ready()

    # for ODS loads, update snapshots
    :ok =
      ready_loads
      |> Enum.filter(&String.starts_with?(&1.s3_key, "cubic/ods_qlik/"))
      |> Enum.each(&CubicOdsLoadSnapshot.update_snapshot(&1))

    # start ingestion
    ready_loads
    |> chunk_loads(@max_num_of_loads, @max_size_of_loads)
    |> Enum.each(&process_loads/1)
  end

  @spec chunk_loads([CubicLoad.t()], integer(), integer()) :: [[CubicLoad.t(), ...]]
  @doc """
  Chunks the loads up by a size and number maximum, allowing for more efficient job allocation
  """
  def chunk_loads(loads, max_num_of_loads, max_size_of_loads) do
    chunk_fun = fn element, acc ->
      total_acc_s3_size =
        Enum.reduce(acc, 0, fn load, acc_s3_size -> load.s3_size + acc_s3_size end)

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

  @spec process_loads([CubicLoad.t(), ...]) ::
          :ok
  def process_loads([_ | _] = ready_load_chunk) do
    start_ingestion(Enum.map(ready_load_chunk, & &1.id))

    :ok
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
