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

    maybe_controlled_user_api_key =
      Application.fetch_env(:ex_cubic_ingestion, :dmap_controlled_user_api_key)

    maybe_public_user_api_key =
      Application.fetch_env(:ex_cubic_ingestion, :dmap_public_user_api_key)

    insert_fetch_jobs(
      maybe_base_url,
      maybe_controlled_user_api_key,
      maybe_public_user_api_key
    )
  end

  @doc """
  Inserts a fetch job for each DMAP feed.
  """
  @spec insert_fetch_jobs(
          {:ok, String.t()} | :error,
          {:ok, String.t()} | :error,
          {:ok, String.t()} | :error
        ) :: Oban.Worker.result()
  def insert_fetch_jobs(
        {:ok, base_url},
        {:ok, controlled_user_api_key},
        {:ok, public_user_api_key}
      )
      when base_url != "" and controlled_user_api_key != "" and public_user_api_key != "" do
    Repo.transaction(fn ->
      Enum.each(CubicDmapFeed.all(), &Oban.insert(FetchDmap.new(%{feed_id: &1.id})))
    end)

    :ok
  end

  def insert_fetch_jobs(_base_url, _controlled_user_api_key, _public_user_api_key) do
    {:error,
     "dmap_base_url or dmap_controlled_user_api_key or dmap_public_user_api_key empty, dmap will not be scheduled"}
  end
end
