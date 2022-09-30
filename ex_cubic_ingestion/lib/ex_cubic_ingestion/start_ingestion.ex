defmodule ExCubicIngestion.StartIngestion do
  @moduledoc """
  StartIngestion module.
  """

  use GenServer

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.SchemaFetch
  alias ExCubicIngestion.Workers.Ingest

  require Logger
  require Oban
  require Oban.Job

  @wait_interval_ms 60_000
  # maxes for each chunk
  @max_num_of_loads 10
  @max_size_of_loads 100_000_000

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
  Get list of load records that are in 'ready' state, ordered by s3_modified, s3_key,
  and prepare them for processing.
  """
  @spec run(t) :: :ok
  def run(state) do
    ready_loads = CubicLoad.get_status_ready()

    # for ODS loads, update snapshots
    :ok =
      Enum.each(ready_loads, fn {_table_rec, load_rec} ->
        if CubicLoad.ods_load?(load_rec.s3_key) do
          CubicOdsLoadSnapshot.update_snapshot(load_rec)
        end
      end)

    # check schemas, and split into valid and invalid loads
    {valid_ready_loads, invalid_ready_loads} =
      Enum.split_with(ready_loads, &valid_schema?(state, &1))

    # start ingestion for those with valid schemas
    valid_ready_loads
    |> Enum.map(fn {_table_rec, load_rec} -> load_rec end)
    |> start_ingestion()

    # log and error out invalid ones
    Enum.each(invalid_ready_loads, fn {_table_rec, load_rec} = ready_load ->
      Logger.error(
        "[ex_cubic_ingestion] [start_ingestion] Invalid schema detected: #{inspect(ready_load)}"
      )

      CubicLoad.update(load_rec, %{status: "ready_for_erroring"})
    end)

    :ok
  end

  @spec chunk_loads([CubicLoad.t(), ...], integer(), integer()) :: [[CubicLoad.t(), ...], ...]
  @doc """
  Chunks the loads up by a size and number maximum, allowing for more efficient job allocation
  """
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

  @spec valid_schema?(t, {CubicTable.t(), CubicLoad.t()}) :: boolean()
  defp valid_schema?(state, {table_rec, load_rec}) do
    # if ODS, check the schema provided by Cubic (.dfm files) against Glue
    if CubicLoad.ods_load?(load_rec.s3_key) do
      SchemaFetch.get_cubic_ods_qlik_columns(state.lib_ex_aws, load_rec) ==
        SchemaFetch.get_glue_columns(state.lib_ex_aws, table_rec, load_rec)
    else
      # @todo implement DMAP schema checker once we have the schema API from Cubic
      true
    end
  end

  @spec add_ingest_job([integer(), ...]) :: {atom(), map()}
  defp add_ingest_job(load_rec_ids) do
    Ecto.Multi.new()
    |> Ecto.Multi.update_all(
      :update_status,
      CubicLoad.query_many(load_rec_ids),
      set: [status: "ingesting"]
    )
    |> Oban.insert(:ingest_job, Ingest.new(%{load_rec_ids: load_rec_ids}))
    |> Repo.transaction()
  end

  @spec start_ingestion([CubicLoad.t(), ...]) :: :ok
  defp start_ingestion(load_recs) do
    load_recs
    |> chunk_loads(@max_num_of_loads, @max_size_of_loads)
    |> Enum.map(&Enum.map(&1, fn load_rec -> load_rec.id end))
    |> Enum.each(&add_ingest_job/1)
  end
end
