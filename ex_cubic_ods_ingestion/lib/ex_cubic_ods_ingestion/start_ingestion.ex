defmodule ExCubicOdsIngestion.StartIngestion do
  @moduledoc """
  StartIngestion module.
  """

  use GenServer

  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Schema.CubicOdsTable

  require Logger
  require ExAws.S3

  import Ecto.Query

  @wait_interval_ms 5_000

  # client methods
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, [])
  end

  def status(server) do
    GenServer.call(server, :status)
  end

  # callbacks
  @impl true
  def init(_opts) do
    {:ok, %{status: :running}, 0}
  end

  @impl true
  def handle_info(:timeout, %{} = state) do
    new_state = run(state)

    # set timeout according to need for continuing
    timeout = @wait_interval_ms

    {:noreply, new_state, timeout}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, state[:status], state}
  end

  # server helper functions
  @spec run(map()) :: map()
  defp run(state) do

    # get available tables
    table_recs = CubicOdsTable.get_all()

    # get list of load records that are in 'ready' state, ordered by s3_modified, s3_key
    ready_load_recs = CubicOdsLoad.get_status_ready()

    # attach table info to load records
    ready_loads = Enum.map(ready_load_recs, &attach_table_info(&1, table_recs))

    # iterate through the load records list in order to start ingesting job,
    # or error
    Enum.each(ready_loads, &start_ingestion(&1))

    # return
    state
  end

  def attach_table_info(load_rec, table_recs) do
    # find the table rec that the load is for
    table_rec = if load_rec.table_id do
      Enum.find(table_recs, &find_table_rec_by_id(&1, load_rec))
    else
      Enum.find(table_recs, &find_table_rec_by_s3_prefix(&1, load_rec))
    end

    # update snapshot for table, if needed
    table_rec = CubicOdsTable.update_snapshot(table_rec, load_rec)

    {load_rec, table_rec}
  end

  def start_ingestion({load_rec, table_rec}) do
    if table_rec do
      Logger.info("---- start job")
    else
      ProcessIngestion.error(load_rec)
    end
  end

  def find_table_rec_by_id(table_rec, load_rec) do
    table_rec.id == load_rec.table_id
  end

  def find_table_rec_by_s3_prefix(table_rec, load_rec) do
    # get just the s3 prefix from load rec
    load_s3_prefix = load_rec[:s3_key] |> Path.dirname()
    # if cdc, we want to strip off the '__ct'
    load_s3_prefix = if String.ends_with?(load_s3_prefix, "__ct") do
      String.replace(load_s3_prefix, "__ct", "")
    else
      load_s3_prefix
    end

    # return true if we have a match
    table_rec[:s3_prefix] == "#{load_s3_prefix}/"
  end
end
