defmodule ExCubicIngestion.Workers.ScheduleDmapTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicDmapFeed
  alias ExCubicIngestion.Workers.FetchDmap
  alias ExCubicIngestion.Workers.ScheduleDmap

  describe "perform/1" do
    test "run job without error" do
      Repo.insert!(%CubicDmapFeed{
        relative_url: "/controlledresearchusersapi/sample1"
      })

      assert :ok == perform_job(ScheduleDmap, %{})
    end

    test "fetch dmap jobs are queued" do
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

      :ok = perform_job(ScheduleDmap, %{})

      assert_enqueued(worker: FetchDmap, args: %{feed_id: dmap_feed_1.id})

      assert_enqueued(worker: FetchDmap, args: %{feed_id: dmap_feed_2.id})

      refute_enqueued(worker: FetchDmap, args: %{feed_id: dmap_feed_deleted.id})
    end
  end
end
