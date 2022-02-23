defmodule ExCubicOdsIngestion.StartIngestion do
  @moduledoc """
  StartIngestion module.
  """

  use GenServer

  alias ExCubicOdsIngestion.ProcessIngestion
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Schema.CubicOdsTable
  alias ExCubicOdsIngestion.Workers.Ingest

  require Logger
  require Oban

  @wait_interval_ms 5_000

  defstruct [:lib_ex_aws, status: :not_started, continuation_token: "", max_keys: 1_000]

  # client methods
  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    # define lib_ex_aws, unless it's already defined
    opts = Keyword.put_new(opts, :lib_ex_aws, ExAws)

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
    ready_load_recs = CubicOdsLoad.get_status_ready()

    # attach table to load records, and update snapshots
    ready_loads = Enum.map(ready_load_recs, &attach_table(&1))

    # iterate through the load records list in order to start ingesting job,
    # or error
    Enum.each(ready_loads, &start_ingestion(&1))

    # return
    state
  end

  @spec attach_table(CubicOdsLoad.t()) :: tuple()
  def attach_table(load_rec) do
    # find the table rec that the load is for
    table_rec =
      if load_rec.table_id do
        CubicOdsTable.get(load_rec.table_id)
      else
        CubicOdsTable.get_from_load_s3_key(load_rec.s3_key)
      end

    if table_rec do
      # for table, update snapshot if we have a new snapshot, as in
      # the load key matches the snapshot key and the load modified date is newer than the snapshot
      table_rec =
        if table_rec.snapshot_s3_key == load_rec.s3_key and
             table_rec.snapshot < load_rec.s3_modified do
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

  @spec start_ingestion(tuple()) :: :ok
  def start_ingestion({load_rec, table_rec}) do
    if table_rec do
      # update status to ingesting
      CubicOdsLoad.update(load_rec, %{status: "ingesting"})

      # queue for ingesting
      %{load: load_rec, table: table_rec} |> Ingest.new() |> Oban.insert()
    else
      ProcessIngestion.error(load_rec)
    end

    :ok
  end
end
