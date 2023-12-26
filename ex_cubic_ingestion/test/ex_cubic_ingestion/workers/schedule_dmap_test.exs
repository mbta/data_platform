defmodule ExCubicIngestion.Workers.ScheduleDmapTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicDmapFeed
  alias ExCubicIngestion.Workers.FetchDmap
  alias ExCubicIngestion.Workers.ScheduleDmap

  describe "perform/1" do
    test "fetch dmap job is enqueued correctly" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/sample"
        })

      assert :ok = perform_job(ScheduleDmap, %{})

      assert_enqueued(worker: FetchDmap, args: %{feed_id: dmap_feed.id})
    end
  end

  describe "insert_fetch_jobs/3" do
    test "fetch dmap jobs are inserted" do
      dmap_feed_1 =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/sample1"
        })

      dmap_feed_2 =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/sample2"
        })

      dmap_feed_deleted =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/deleted",
          deleted_at: ~U[2022-05-01 10:49:50Z]
        })

      assert :ok =
               ScheduleDmap.insert_fetch_jobs(
                 {:ok, "https://dmap_base_url"},
                 {:ok, "controlled_api_key"},
                 {:ok, "public_api_key"}
               )

      assert_enqueued(worker: FetchDmap, args: %{feed_id: dmap_feed_1.id})

      assert_enqueued(worker: FetchDmap, args: %{feed_id: dmap_feed_2.id})

      refute_enqueued(worker: FetchDmap, args: %{feed_id: dmap_feed_deleted.id})
    end

    test "fetch dmap job is not inserted when env varibles are not set" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/sample"
        })

      assert {:error, _base_url_not_set} =
               ScheduleDmap.insert_fetch_jobs(
                 {:ok, ""},
                 {:ok, "controlled_api_key"},
                 {:ok, "public_api_key"}
               )

      assert {:error, _controlled_api_key_not_set} =
               ScheduleDmap.insert_fetch_jobs(
                 {:ok, "https://dmap_base_url"},
                 {:ok, ""},
                 {:ok, "public_api_key"}
               )

      assert {:error, _public_api_key_not_set} =
               ScheduleDmap.insert_fetch_jobs(
                 {:ok, "https://dmap_base_url"},
                 {:ok, "controlled_api_key"},
                 {:ok, ""}
               )

      refute_enqueued(worker: FetchDmap, args: %{feed_id: dmap_feed.id})
    end
  end
end
