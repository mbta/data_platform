defmodule ReleaseTasks.ScheduleDmap do
  @moduledoc """
  This task will insert the ScheduleDmap job upon execution, resulting in
  the fetching of all DMAP feeds.

  Based on: https://hexdocs.pm/phoenix/releases.html#ecto-migrations-and-custom-commands
  """

  alias ExCubicIngestion.Workers.ScheduleDmap

  require Logger

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

  @spec oban_config :: map
  def oban_config do
    dmap_is_configured =
      [
        Application.fetch_env(:ex_cubic_ingestion, :dmap_base_url),
        Application.fetch_env(:ex_cubic_ingestion, :dmap_api_key)
      ]
      |> Enum.map(fn
        :error -> false
        {:ok, ""} -> false
        _ok -> true
      end)
      |> Enum.all?(fn x -> x end)

    case dmap_is_configured do
      true ->
        Application.fetch_env!(:ex_cubic_ingestion, Oban)

      false ->
        Logger.warning("dmap_base_url or dmap_api_key empty, dmap will not be scheduled")

        Enum.map(Application.fetch_env!(:ex_cubic_ingestion, Oban), fn
          {:plugins, _} -> {:plugins, false}
          item -> item
        end)
    end
  end
end
