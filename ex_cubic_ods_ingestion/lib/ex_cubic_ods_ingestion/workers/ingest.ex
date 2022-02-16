defmodule ExCubicOdsIngestion.Workers.Ingest do
  use Oban.Worker,
    queue: :ingest,
    max_attempts: 3

  @job_timeout_in_sec 30

  @impl Oban.Worker
  def perform(%Oban.Job{attempt: attempt}) when attempt > 3 do
    IO.inspect(attempt)
  end

  def perform(job) do
    IO.inspect(job.args)
  end

  @impl Oban.Worker
  def timeout(_job), do: :timer.seconds(@job_timeout_in_sec)
end
