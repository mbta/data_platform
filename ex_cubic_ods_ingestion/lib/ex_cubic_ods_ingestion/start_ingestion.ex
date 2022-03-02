defmodule ExCubicOdsIngestion.StartIngestion do
  @moduledoc """
  StartIngestion module.
  """

  use GenServer

  alias ExCubicOdsIngestion.ProcessIngestion
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
    # get list of load records that are in 'ready' state, ordered by s3_modified, s3_key
    # prepare them for processing, and kick off separate flows
    CubicOdsLoad.get_status_ready() |> prepare_loads() |> process_loads()

    # return
    state
  end

  @spec prepare_loads([CubicOdsLoad.t()]) ::
          {[{CubicOdsLoad.t(), nil}], [[{CubicOdsLoad.t(), CubicOdsTable.t()}]]}
  def prepare_loads(load_recs) do
    # attach table to load records, add or update any snapshots
    loads = Enum.map(load_recs, &attach_table(&1))

    {error_loads, ready_loads} =
      Enum.split_with(loads, fn {_load_rec, table_rec} -> is_nil(table_rec) end)

    # @todo replace chunk_every with chunk_while for more fine-tuned control
    {error_loads, Enum.chunk_every(ready_loads, 3)}
  end

  @spec process_loads({[{CubicOdsLoad.t(), nil}], [[{CubicOdsLoad.t(), CubicOdsTable.t()}]]}) ::
          :ok
  def process_loads({error_loads, ready_load_chunks}) do
    # error out the error loads
    if Enum.count(error_loads) > 0 do
      ProcessIngestion.error(Enum.map(error_loads, fn {load_rec, _table_rec} -> load_rec.id end))
    end

    # start ingestion for the rest
    Enum.each(
      ready_load_chunks,
      &start_ingestion(Enum.map(&1, fn {load_rec, _table_rec} -> load_rec.id end))
    )
  end

  @spec attach_table(CubicOdsLoad.t()) :: tuple()
  def attach_table(load_rec) do
    # find the table rec that the load is for
    table_rec =
      if load_rec.table_id do
        CubicOdsTable.get!(load_rec.table_id)
      else
        CubicOdsTable.get_from_load_s3_key(load_rec.s3_key)
      end

    if table_rec do
      # for table, update snapshot if we have a new snapshot, as in
      # the load key matches the snapshot key and the load modified date is newer than the snapshot
      table_rec =
        if table_rec.snapshot_s3_key == load_rec.s3_key and
             (is_nil(table_rec.snapshot) or
                DateTime.compare(table_rec.snapshot, load_rec.s3_modified) == :lt) do
          CubicOdsTable.update(table_rec, %{snapshot: load_rec.s3_modified})
        else
          table_rec
        end

      # for load, update table_id and snapshot if we don't have one already, as we don't want to override
      load_rec =
        if load_rec.table_id do
          load_rec
        else
          CubicOdsLoad.update(load_rec, %{table_id: table_rec.id, snapshot: table_rec.snapshot})
        end

      {load_rec, table_rec}
    else
      {load_rec, nil}
    end
  end

  @spec start_ingestion([integer()]) :: {:ok, Oban.Job.t()}
  def start_ingestion(load_rec_ids) do
    # update status to ingesting
    CubicOdsLoad.update_many(load_rec_ids, status: "ingesting")

    # queue for ingesting
    %{load_rec_ids: load_rec_ids} |> Ingest.new() |> Oban.insert()
  end
end
