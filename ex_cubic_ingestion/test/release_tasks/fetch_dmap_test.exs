defmodule ReleaseTasks.FetchDmapTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicDmapFeed
  alias ExCubicIngestion.Workers.FetchDmap

  describe "run/1" do
    test "job was queued up" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/sample"
        })

      args = %{feed_id: dmap_feed.id, last_updated: ~U[2022-01-01 10:49:50.123456Z]}

      assert :ok = ReleaseTasks.FetchDmap.run(args)

      assert_enqueued(worker: FetchDmap, args: args)
    end
  end
end
