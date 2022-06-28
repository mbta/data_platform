defmodule ExCubicIngestion.Workers.FetchDmapTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicDmapDataset
  alias ExCubicIngestion.Schema.CubicDmapFeed
  alias ExCubicIngestion.Workers.FetchDmap

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
               FetchDmap.construct_feed_url(dmap_feed, nil)
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
               FetchDmap.construct_feed_url(dmap_feed, nil)
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
               FetchDmap.construct_feed_url(dmap_feed, DateTime.to_string(last_updated))
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
                 FetchDmap.get_feed_datasets(
                   dmap_feed,
                   DateTime.to_string(last_updated),
                   MockHTTPoison
                 ),
                 & &1["dataset_id"]
               )
    end
  end

  describe "fetch_and_upload_to_s3/1" do
    test "getting file and uploading through mocks" do
      dataset_rec = %CubicDmapDataset{
        type: "sample",
        identifier: "sample_20220517"
      }

      dataset_url =
        "https://mbtaqadmapdatalake.blob.core.windows.net/sample/sample_2022-05-17.csv.gz"

      assert dataset_rec ==
               FetchDmap.fetch_and_upload_to_s3(
                 {dataset_rec, dataset_url},
                 MockExAws,
                 MockHTTPoison
               )
    end
  end
end
