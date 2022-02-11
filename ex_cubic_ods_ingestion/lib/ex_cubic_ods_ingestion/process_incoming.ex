defmodule ExCubicOdsIngestion.ProcessIncoming do
  @moduledoc """
  ProcessIncoming module.
  """

  use GenServer

  require Logger
  require ExAws.S3

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
    timeout =
      if new_state[:continuation_token] == "" do
        @wait_interval_ms
      else
        0
      end

    {:noreply, new_state, timeout}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, state[:status], state}
  end

  # server helper functions
  @spec run(map()) :: map()
  defp run(state) do
    # add or update continuation_token
    Map.merge(state, %{continuation_token: ""})
  end
end
