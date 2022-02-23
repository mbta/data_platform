defmodule ExCubicOdsIngestion.Workers.Ingest do
  @moduledoc """
  Workers.Ingest module.
  """

  use Oban.Worker,
    queue: :ingest,
    max_attempts: 3

  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  require Logger

  @job_timeout_in_sec 30

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"load" => load, "table" => _table} = _args}) do
    Process.sleep(2000)

    load_rec = CubicOdsLoad.get(load["id"])
    CubicOdsLoad.update(load_rec, status: "ingested")

    :ok
  end

  @impl Oban.Worker
  def timeout(_job), do: :timer.seconds(@job_timeout_in_sec)
end
