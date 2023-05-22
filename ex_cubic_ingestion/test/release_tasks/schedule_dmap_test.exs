defmodule ReleaseTasks.ScheduleDmapTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Workers.ScheduleDmap

  describe "run/1" do
    test "job was queued up" do
      args = %{test: true}

      assert :ok = ReleaseTasks.ScheduleDmap.run(args)

      assert_enqueued(worker: ScheduleDmap, args: args)
    end

    test "oban config items not defined" do
      assert Keyword.fetch!(ReleaseTasks.ScheduleDmap.oban_config(), :plugins) == false
    end
  end
end
