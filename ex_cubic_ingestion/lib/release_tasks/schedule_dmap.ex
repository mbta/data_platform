defmodule ReleaseTasks.ScheduleDmap do
  @moduledoc """
  This task will insert the ScheduleDmap job upon execution, resulting in
  the fetching of all DMAP feeds.

  Based on: https://hexdocs.pm/phoenix/releases.html#ecto-migrations-and-custom-commands
  """

  alias ExCubicIngestion.Workers.ScheduleDmap

  @doc """
  Inserts a ScheduleDMAP job for Oban to process.
  """
  @spec run(map()) :: :ok
  def run(args) do
    ReleaseTasks.Utilities.start_app()

    # add a new schedule dmap job
    Oban.insert!(ScheduleDmap.new(args))

    :ok
  end
end
