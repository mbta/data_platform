defmodule ExCubicIngestion.ObanLogger do
  @moduledoc """
  This module allows for logging more specific messages from Oban's telemetry.
  """

  alias ExCubicIngestion.Schema.CubicDmapFeed
  alias ExCubicIngestion.Schema.CubicLoad

  require Logger

  @log_prefix "[ex_cubic_ingestion] [workers]"

  @spec handle_event(
          :telemetry.event_name(),
          :telemetry.event_measurements(),
          :telemetry.event_metadata(),
          :telemetry.handler_config()
        ) :: any()
  def handle_event([:oban, :job, :start], _measure, meta, _config) do
    Logger.info(
      "#{@log_prefix} [#{meta.queue}] Start: args=#{inspect(meta.args, charlists: :as_lists)}"
    )
  end

  def handle_event([:oban, :job, :stop], measure, meta, _config) do
    Logger.info(
      "#{@log_prefix} [#{meta.queue}] Stop: args=#{inspect(meta.args, charlists: :as_lists)} duration=#{measure.duration} queue_time=#{measure.queue_time} state=#{meta.state} attempt=#{meta.attempt}"
    )
  end

  def handle_event(
        [:oban, :job, :exception],
        measure,
        %{worker: worker} = meta,
        _config
      )
      when worker in ["ExCubicIngestion.Workers.Archive", "ExCubicIngestion.Workers.Error"] do
    %{"load_rec_id" => load_rec_id} = meta.args

    args_info = CubicLoad.get!(load_rec_id)

    log_exception(measure, meta, args_info)
  end

  def handle_event(
        [:oban, :job, :exception],
        measure,
        %{worker: "ExCubicIngestion.Workers.FetchDmap"} = meta,
        _config
      ) do
    %{"feed_id" => feed_id} = meta.args

    args_info = CubicDmapFeed.get!(feed_id)

    log_exception(measure, meta, args_info)
  end

  def handle_event(
        [:oban, :job, :exception],
        measure,
        %{worker: "ExCubicIngestion.Workers.Ingest"} = meta,
        _config
      ) do
    %{"load_rec_ids" => load_rec_ids} = meta.args

    args_info = CubicLoad.get_many_with_table(load_rec_ids)

    log_exception(measure, meta, args_info)
  end

  def handle_event([:oban, :job, :exception], measure, meta, _config) do
    log_exception(measure, meta, {})
  end

  defp log_exception(measure, meta, args_info) do
    Logger.error(
      "#{@log_prefix} [#{meta.queue}] Exception: args=#{inspect(meta.args, charlists: :as_lists)} duration=#{measure.duration} queue_time=#{measure.queue_time} state=#{meta.state} attempt=#{meta.attempt} kind=#{meta.kind} error=#{inspect(meta.error)}\nargs_info: #{inspect(args_info)}\nStacktrace:\n#{Exception.format_stacktrace(meta.stacktrace)}"
    )
  end
end
