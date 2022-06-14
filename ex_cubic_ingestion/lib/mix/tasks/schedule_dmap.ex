defmodule Mix.Tasks.ScheduleDmap do
  @moduledoc """
  This task will insert the ScheduleDmap job upon execution, resulting in
  the fetching of all DMAP feeds.
  """
  use Mix.Task

  alias ExCubicIngestion.Workers.ScheduleDmap

  require Oban

  @doc """
  Start the application without the GenServers, so we don't have competing
  processes for querying the incoming bucket, and then inserting a ScheduleDMAP
  job for Oban to process.
  """
  @impl Mix.Task
  def run(_args) do
    Application.put_env(:ex_cubic_ingestion, :start_app_children?, false)

    Mix.Task.run("app.start")

    Oban.insert!(ScheduleDmap.new(%{}))
  end
end
