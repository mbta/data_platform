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
    maybe_base_url = Application.fetch_env(:ex_cubic_ingestion, :dmap_base_url)
    maybe_api_key = Application.fetch_env(:ex_cubic_ingestion, :dmap_base_url)

    case {maybe_base_url, maybe_api_key} do
      {{:ok, base_url}, {:ok, api_key}} when base_url != "" and api_key != "" ->
        Repo.transaction(fn ->
          Enum.each(CubicDmapFeed.all(), &Oban.insert(FetchDmap.new(%{feed_id: &1.id})))
        end)

        :ok

      _error ->
        {:error, "dmap_base_url or dmap_api_key empty, dmap will not be scheduled"}
    end
  end
end
