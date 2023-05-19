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
    dmap_base_url = Application.fetch_env!(:ex_cubic_ingestion, :dmap_base_url)
    dmap_api_key = Application.fetch_env!(:ex_cubic_ingestion, :dmap_api_key)

    if dmap_base_url == "" or dmap_api_key == "" do
      {:error, "dmap_base_url or dmap_api_key is empty, not running dmap job"}
    else
      Repo.transaction(fn ->
        Enum.each(CubicDmapFeed.all(), &Oban.insert(FetchDmap.new(%{feed_id: &1.id})))
      end)

      :ok
    end
  rescue
    e in ArgumentError ->
      {:error, "dmap_base_url or dmap_api_key undefined, not running dmap job: " <> e.message}
  end
end
