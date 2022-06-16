defmodule ReleaseTasks.ScheduleDmap do
  @moduledoc """
  This task will insert the ScheduleDmap job upon execution, resulting in
  the fetching of all DMAP feeds.

  Based on: https://hexdocs.pm/phoenix/releases.html#ecto-migrations-and-custom-commands
  """

  alias ExCubicIngestion.Workers.ScheduleDmap

  @app :ex_cubic_ingestion

  @doc """
  Start the application without the GenServers, so we don't have competing
  processes for querying the incoming bucket, and then inserting a ScheduleDMAP
  job for Oban to process.
  """
  @spec run(map()) :: :ok
  def run(args) do
    # loads application configuration
    Application.load(@app)

    # disables running the full app and just start Oban and Ecto
    Application.put_env(@app, :start_app_children?, false)

    # starts app
    Application.ensure_all_started(@app)

    # add a new schedule dmap job
    Oban.insert!(ScheduleDmap.new(args))

    :ok
  end
end
