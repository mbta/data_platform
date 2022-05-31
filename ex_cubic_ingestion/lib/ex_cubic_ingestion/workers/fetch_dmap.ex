defmodule ExCubicIngestion.Workers.FetchDmap do
  @moduledoc """
  Oban Worker for fetching a DMAP and the data files available in that feed, ultimately
  uploading them to the 'Incoming' bucket for further processing through the ingestion
  process.
  """

  use Oban.Worker,
    queue: :fetch_dmap,
    max_attempts: 1

  alias ExCubicIngestion.Schema.CubicDmapDataset
  alias ExCubicIngestion.Schema.CubicDmapFeed
  alias ExCubicIngestion.Validators

  require Logger

  @impl Oban.Worker
  def perform(%{args: args} = _job) do
    # extract required information
    %{"feed_id" => feed_id} = args
    # extract opitional information
    last_updated = Map.get(args, "last_updated")

    # allow for ex_aws module to be passed in as a string, since Oban will need to
    # serialize args to JSON. defaulted to library module.
    _lib_ex_aws =
      case args do
        %{"lib_ex_aws" => mod_str} -> Module.safe_concat([mod_str])
        _args_lib_ex_aws -> ExAws
      end

    # allow for httpoison module to be passed in as a string, since Oban will need to
    # serialize args to JSON. defaulted to library module.
    lib_httpoison =
      case args do
        %{"lib_httpoison" => mod_str} -> Module.safe_concat([mod_str])
        _args_lib_httpoison -> HTTPoison
      end

    feed_rec = CubicDmapFeed.get!(feed_id)

    feed_rec
    |> construct_feed_url(last_updated)
    |> get_feed(lib_httpoison)
    |> Map.get("results", [])
    |> Enum.filter(&is_valid_dataset(&1))
    |> CubicDmapDataset.upsert_many_from_datasets(feed_rec)
    |> Enum.map(&fetch_and_upload_to_s3(&1))
    |> update_last_updated_for_feed(feed_rec)

    :ok
  end

  @doc """
  Construct the full URL to the feed, applying some overrriding logic for
  last updated (if passed in).
  """
  @spec construct_feed_url(CubicDmapFeed.t(), DateTime.t()) :: String.t()
  def construct_feed_url(feed_rec, last_updated \\ nil) do
    dmap_base_url = Application.fetch_env!(:ex_cubic_ingestion, :dmap_base_url)

    dmap_api_key = Application.fetch_env!(:ex_cubic_ingestion, :dmap_api_key)

    last_updated_query_param =
      cond do
        not is_nil(last_updated) ->
          "&last_updated=#{Calendar.strftime(last_updated, "%Y-%m-%dT%H:%M:%S.%f")}"

        not is_nil(feed_rec.last_updated_at) ->
          "&last_updated=#{Calendar.strftime(DateTime.add(feed_rec.last_updated_at, 1, :microsecond), "%Y-%m-%dT%H:%M:%S.%f")}"

        true ->
          ""
      end

    "#{dmap_base_url}#{feed_rec.relative_url}?apikey=#{dmap_api_key}#{last_updated_query_param}"
  end

  @doc """
  Make sure that the dataset has all the required fields and has valid data.
  """
  @spec is_valid_dataset(map()) :: boolean()
  def is_valid_dataset(dataset_map) do
    Validators.map_has_keys?(dataset_map, [
      "id",
      "dataset_id",
      "start_date",
      "end_date",
      "last_updated",
      "url"
    ]) &&
      Validators.is_valid_iso_date?(dataset_map["start_date"]) &&
      Validators.is_valid_iso_date?(dataset_map["end_date"]) &&
      Validators.is_valid_iso_datetime?(dataset_map["last_updated"]) &&
      Validators.is_valid_dmap_dataset_url?(dataset_map["url"])
  end

  @doc """
  @todo
  """
  @spec fetch_and_upload_to_s3([CubicDmapDataset.t()]) :: [CubicDmapDataset.t()]
  def fetch_and_upload_to_s3(dataset_recs) do
    # @todo

    dataset_recs
  end

  @doc """
  @todo
  """
  @spec update_last_updated_for_feed([CubicDmapDataset.t()], CubicDmapFeed.t()) :: :ok
  def update_last_updated_for_feed(_dataset_recs, _feed_rec) do
    # @todo

    :ok
  end

  @spec get_feed(String.t(), module()) :: map()
  defp get_feed(url, lib_httpoison) do
    %HTTPoison.Response{status_code: 200, body: body} = lib_httpoison.get!(url)

    Jason.decode!(body)
  end
end
