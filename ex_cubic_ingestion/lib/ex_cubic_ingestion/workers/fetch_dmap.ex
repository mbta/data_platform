defmodule ExCubicIngestion.Workers.FetchDmap do
  @moduledoc """
  Oban Worker for fetching a DMAP feed and the data files available in that feed, ultimately
  uploading them to the 'Incoming' bucket for further processing through the ingestion
  process.
  """

  use Oban.Worker,
    queue: :fetch_dmap,
    max_attempts: 1

  alias ExCubicIngestion.Schema.CubicDmapDataset
  alias ExCubicIngestion.Schema.CubicDmapFeed

  @impl Oban.Worker
  def perform(%{args: args} = _job) do
    # extract required information
    %{"feed_id" => feed_id} = args
    # extract optional information
    last_updated = Map.get(args, "last_updated")

    # allow for ex_aws module to be passed in as a string, since Oban will need to
    # serialize args to JSON. defaulted to library module.
    lib_ex_aws =
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
    |> get_feed_datasets(last_updated, lib_httpoison)
    |> CubicDmapDataset.upsert_many_from_datasets(feed_rec)
    |> Enum.map(&fetch_and_upload_to_s3(&1, lib_ex_aws, lib_httpoison))
    |> CubicDmapFeed.update_last_updated_from_datasets(feed_rec)

    :ok
  end

  @doc """
  Construct the full URL to the feed, with the correct API key, and applying some
  overriding logic for last updated (if passed in).
  """
  @spec construct_feed_url(CubicDmapFeed.t(), String.t(), String.t() | nil) :: String.t()
  def construct_feed_url(feed_rec, dmap_api_key, last_updated) do
    dmap_base_url = Application.fetch_env!(:ex_cubic_ingestion, :dmap_base_url)

    last_updated =
      cond do
        not is_nil(last_updated) ->
          CubicDmapDataset.iso_extended_to_datetime(last_updated)

        not is_nil(feed_rec.last_updated_at) ->
          DateTime.add(feed_rec.last_updated_at, 1, :microsecond)

        true ->
          nil
      end

    last_updated_query_param =
      if last_updated do
        "&last_updated=#{Calendar.strftime(last_updated, "%Y-%m-%dT%H:%M:%S.%f")}"
      else
        ""
      end

    "#{dmap_base_url}#{feed_rec.relative_url}?apikey=#{dmap_api_key}#{last_updated_query_param}"
  end

  @doc """
  Using the feed record to construct a URL and get the contents containing the dataset
  information. Also, checks that datasets are valid and filters out invalid ones.
  """
  @spec get_feed_datasets(CubicDmapFeed.t(), String.t() | nil, module()) :: [map()]
  def get_feed_datasets(feed_rec, last_updated, lib_httpoison) do
    dmap_api_key =
      cond do
        String.starts_with?(feed_rec.relative_url, "/datasetcontrolleduserapi") ->
          Application.fetch_env!(:ex_cubic_ingestion, :dmap_controlled_user_api_key)

        String.starts_with?(feed_rec.relative_url, "/datasetpublicusersapi") ->
          Application.fetch_env!(:ex_cubic_ingestion, :dmap_public_user_api_key)

        true ->
          raise "No API key available."
      end

    body =
      case lib_httpoison.get(construct_feed_url(feed_rec, dmap_api_key, last_updated),
             apikey: dmap_api_key
           ) do
        {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
          body

        {:ok, %HTTPoison.Response{body: body}} ->
          raise "Unable to fetch feed results: #{feed_rec.relative_url} (Response: #{inspect(body)})"

        _exception_or_error_code ->
          raise "Unable to fetch feed results: #{feed_rec.relative_url}"
      end

    body
    |> Jason.decode!()
    |> Map.get("results", [])
    |> Enum.filter(&CubicDmapDataset.valid_dataset?(&1))
  end

  @doc """
  For the dataset, download data with the URL provided, and upload to Incoming bucket.
  """
  @spec fetch_and_upload_to_s3({CubicDmapDataset.t(), String.t()}, module(), module()) ::
          CubicDmapDataset.t()
  def fetch_and_upload_to_s3({dataset_rec, dataset_url}, lib_ex_aws, lib_httpoison) do
    bucket_incoming = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_incoming)

    prefix_incoming = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    dataset_url
    |> Downloader.stream!(lib_httpoison)
    |> ExAws.S3.upload(
      bucket_incoming,
      "#{prefix_incoming}cubic/dmap/#{dataset_rec.type}/#{dataset_rec.identifier}.csv.gz"
    )
    |> lib_ex_aws.request!()

    dataset_rec
  end
end
