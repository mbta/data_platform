defmodule App.ProcessIncoming do
  use GenServer
  require Logger

  @wait_interval_ms 1_000

  # main function
  def run() do

    # simulate delay
    Process.sleep(5_000)

    # recursively run
    GenServer.cast(__MODULE__, {:run})

    {:ok}
  end


  # genserver herlpers
  # start genserver
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, [])
  end

  # get genserver status
  def status(server) do
    GenServer.call(server, :status)
  end


  # GenServer Callbacks
  @impl true
  def init(_opts) do
    # wait (1s) for the server to initialize before sending a start message
    Process.send_after(self(), :start, @wait_interval_ms)

    {:ok, %{ status: :starting }}
  end

  @impl true
  def handle_info(:start, %{status: :starting} = _state) do
    # kick off the first run
    __MODULE__.run()

    {:noreply, %{ status: :running }}
  end

  @impl true
  def handle_cast({:run}, state) do
    __MODULE__.run()

    {:noreply, state}
  end

  # @todo implement :DOWN

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, state[:status], state}
  end

end
