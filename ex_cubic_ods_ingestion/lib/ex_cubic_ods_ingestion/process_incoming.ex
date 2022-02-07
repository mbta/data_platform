defmodule ExCubicOdsIngestion.ProcessIncoming do
  @moduledoc """
  ProcessIncoming module.
  """

  use GenServer

  alias ExCubicOdsIngestion.Repo

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
    run()

    timeout = @wait_interval_ms

    {:noreply, state, timeout}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, state[:status], state}
  end

  # server helper functions
  defp run do
    bucket = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_operations)
    Logger.info(bucket)

    # %{body: %{contents: contents} } =
    #   ExAws.S3.list_objects(bucket) |> ExAws.request!()

    # contents |> Enum.each(fn e -> IO.puts "Elem: #{e[:key]}" end)

    query = from t in "cubic_ods_tables",
          select: %{name: t.name}
    results = Repo.all(query)

    results |> Enum.each(fn row -> IO.puts "Name: #{row[:name]}" end)

    # ...

    :ok
  end
end
