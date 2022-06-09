defmodule ExCubicIngestion.Workers.ScheduleDmap do
  @moduledoc """
  This worker will be run daily, and it will queue up fetch DMAP jobs for
  each active feed.
  """

  use Oban.Worker,
    queue: :schedule_dmap,
    max_attempts: 1

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicDmapFeed
  alias ExCubicIngestion.Workers.FetchDmap

  require Oban
  require Oban.Job

  @impl Oban.Worker
  def perform(_job) do
    Repo.transaction(fn ->
      Enum.each(CubicDmapFeed.all(), &Oban.insert(FetchDmap.new(%{feed_id: &1.id})))
    end)

    :ok
  end
end
