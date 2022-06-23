defmodule ExCubicIngestion.ObanWorkerError do
  @moduledoc """
  Provides a way for Oban to update status of load upon a complete failure of an 'ingest' job.
  """

  alias ExCubicIngestion.Schema.CubicLoad

  @spec handle_event(
          :telemetry.event_name(),
          :telemetry.event_measurements(),
          :telemetry.event_metadata(),
          :telemetry.handler_config()
        ) :: [CubicLoad.t()]
  @doc """
  Matches on the Ingest worker and when attempts equal max attempts, and updates the status
  of load once all attempts have failed.
  """
  def handle_event(
        [:oban, :job, :exception],
        _measure,
        %{
          worker: "ExCubicIngestion.Workers.Ingest",
          attempt: attempts,
          max_attempts: attempts
        } = meta,
        _config
      ) do
    %{"load_rec_ids" => load_rec_ids} = meta.args

    CubicLoad.update_many(load_rec_ids, status: "ready_for_erroring")
  end

  def handle_event(
        [:oban, :job, :exception],
        _measure,
        %{worker: "ExCubicIngestion.Workers.Archive"} = meta,
        _config
      ) do
    %{"load_rec_id" => load_rec_id} = meta.args

    CubicLoad.update(CubicLoad.get!(load_rec_id), %{status: "archived_unknown"})
  end

  def handle_event(
        [:oban, :job, :exception],
        _measure,
        %{worker: "ExCubicIngestion.Workers.Error"} = meta,
        _config
      ) do
    %{"load_rec_id" => load_rec_id} = meta.args

    CubicLoad.update(CubicLoad.get!(load_rec_id), %{status: "errored_unknown"})
  end

  def handle_event([:oban, :job, :exception], _measure, _meta, _config) do
    []
  end
end
