defmodule ReleaseTasks.FetchDmap do
  @moduledoc """
  Inserts a FetchDmap job for the DMAP feed specified, and the minimum
  last updated datetime that datasets should be fetched for.

  Based on: https://hexdocs.pm/phoenix/releases.html#ecto-migrations-and-custom-commands
  """

  alias ExCubicIngestion.Workers.FetchDmap

  @spec run(map()) :: :ok
  @doc """
  Inserts a FetchDmap job to get the specified feed and get datasets that were
  updated after the last updated parameter.
  """
  def run(%{feed_id: feed_id, last_updated: %DateTime{} = last_updated}) do
    ReleaseTasks.Application.start()

    Oban.insert(
      FetchDmap.new(%{
        feed_id: feed_id,
        last_updated: last_updated
      })
    )

    :ok
  end
end
