defmodule ExCubicIngestion.Workers.FetchDmapTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicDmapDataset
  alias ExCubicIngestion.Schema.CubicDmapFeed
  alias ExCubicIngestion.Workers.FetchDmap

  require MockHTTPoison
  require MockExAws

  describe "perform/1" do
    test "run job without error" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/sample"
        })

      assert :ok ==
               perform_job(FetchDmap, %{
                 feed_id: dmap_feed.id,
                 lib_ex_aws: "MockExAws",
                 lib_httpoison: "MockHTTPoison"
               })
    end
  end

  describe "is_valid_dataset/1" do
    test "with valid dataset" do
      dataset = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/sample",
        "start_date" => "2022-05-17",
        "end_date" => "2022-05-17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      assert FetchDmap.is_valid_dataset(dataset)
    end

    test "with invalid datasets" do
      dataset_missing_field = %{
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/sample",
        "start_date" => "2022-05-17",
        "end_date" => "2022-05-17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      dataset_invalid_start_date = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/sample",
        "start_date" => "2022-05-45",
        "end_date" => "2022-05-17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      dataset_invalid_end_date = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/sample",
        "start_date" => "2022-05-17",
        "end_date" => "2022:05:17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      dataset_invalid_last_updated = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/sample",
        "start_date" => "2022-05-17",
        "end_date" => "2022-05-17",
        "last_updated" => "2022:05:18T12:12:24.897363"
      }

      dataset_invalid_url_wrong_scheme = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "file://mbtaqadmapdatalake.blob.core.windows.net/sample",
        "start_date" => "2022-05-17",
        "end_date" => "2022-05-17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      dataset_invalid_url_empty_path = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net",
        "start_date" => "2022-05-17",
        "end_date" => "2022-05-17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      dataset_invalid_url_invalid_path = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/",
        "start_date" => "2022-05-17",
        "end_date" => "2022-05-17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      dataset_empty = %{}

      refute FetchDmap.is_valid_dataset(dataset_missing_field) ||
               FetchDmap.is_valid_dataset(dataset_invalid_start_date) ||
               FetchDmap.is_valid_dataset(dataset_invalid_end_date) ||
               FetchDmap.is_valid_dataset(dataset_invalid_last_updated) ||
               FetchDmap.is_valid_dataset(dataset_invalid_url_wrong_scheme) ||
               FetchDmap.is_valid_dataset(dataset_invalid_url_empty_path) ||
               FetchDmap.is_valid_dataset(dataset_invalid_url_invalid_path) ||
               FetchDmap.is_valid_dataset(dataset_empty)
    end
  end

  describe "construct_feed_url/2" do
    test "feed without a last updated timestamp" do
      dmap_feed_relative_url = "/controlledresearchusersapi/sample"

      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: dmap_feed_relative_url
        })

      dmap_base_url = Application.fetch_env!(:ex_cubic_ingestion, :dmap_base_url)

      dmap_api_key = Application.fetch_env!(:ex_cubic_ingestion, :dmap_api_key)

      assert "#{dmap_base_url}#{dmap_feed_relative_url}?apikey=#{dmap_api_key}" ==
               FetchDmap.construct_feed_url(dmap_feed)
    end

    test "feed with a last updated timestamp" do
      dmap_feed_relative_url = "/controlledresearchusersapi/sample"

      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: dmap_feed_relative_url,
          last_updated_at: ~U[2022-05-22 20:49:50.123456Z]
        })

      dmap_base_url = Application.fetch_env!(:ex_cubic_ingestion, :dmap_base_url)

      dmap_api_key = Application.fetch_env!(:ex_cubic_ingestion, :dmap_api_key)

      assert "#{dmap_base_url}#{dmap_feed_relative_url}?apikey=#{dmap_api_key}&last_updated=2022-05-22T20:49:50.123457" ==
               FetchDmap.construct_feed_url(dmap_feed)
    end

    test "feed with last updated passed in" do
      dmap_feed_relative_url = "/controlledresearchusersapi/sample"

      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: dmap_feed_relative_url,
          last_updated_at: ~U[2022-05-22 20:49:50.123456Z]
        })

      last_updated = ~U[2022-05-01 10:49:50.123456Z]

      dmap_base_url = Application.fetch_env!(:ex_cubic_ingestion, :dmap_base_url)

      dmap_api_key = Application.fetch_env!(:ex_cubic_ingestion, :dmap_api_key)

      assert "#{dmap_base_url}#{dmap_feed_relative_url}?apikey=#{dmap_api_key}&last_updated=2022-05-01T10:49:50.123456" ==
               FetchDmap.construct_feed_url(dmap_feed, last_updated)
    end
  end

  describe "get_feed_datasets/3" do
    test "getting mock feed results" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/sample",
          last_updated_at: ~U[2022-05-22 20:49:50.123456Z]
        })

      last_updated = ~U[2022-05-01 10:49:50.123456Z]

      assert ["sample_20220517", "sample_20220518"] =
               Enum.map(
                 FetchDmap.get_feed_datasets(dmap_feed, last_updated, MockHTTPoison),
                 & &1["dataset_id"]
               )
    end
  end

  describe "fetch_and_upload_to_s3/1" do
    test "getting file and uploading through mocks" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/sample",
          last_updated_at: ~U[2022-05-22 20:49:50.123456Z]
        })

      dmap_dataset =
        Repo.insert!(%CubicDmapDataset{
          feed_id: dmap_feed.id,
          type: "sample",
          identifier: "sample_20220517",
          start_date: ~D[2022-05-17],
          end_date: ~D[2022-05-17],
          last_updated_at: ~U[2022-05-18 12:12:24.897363Z]
        })

      dataset_url =
        "https://mbtaqadmapdatalake.blob.core.windows.net/sample/sample_2022-05-17.csv.gz"

      assert dmap_dataset ==
               FetchDmap.fetch_and_upload_to_s3(
                 {dmap_dataset, dataset_url},
                 MockExAws,
                 MockHTTPoison
               )
    end
  end
end
