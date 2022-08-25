defmodule ReleaseTasks.RetryArchiveErrorTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Workers.Archive
  alias ExCubicIngestion.Workers.Error

  describe "run/1" do
    test "jobs are queued up" do
      table =
        Repo.insert!(%CubicTable{
          name: "cubic_dmap__sample",
          s3_prefix: "cubic/dmap/sample/"
        })

      # insert loads
      load_1 =
        Repo.insert!(%CubicLoad{
          table_id: table.id,
          status: "archiving",
          s3_key: "cubic/dmap/sample/20220101.csv.gz",
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197
        })

      load_2 =
        Repo.insert!(%CubicLoad{
          table_id: table.id,
          status: "archived_unknown",
          s3_key: "cubic/dmap/sample/20220102.csv.gz",
          s3_modified: ~U[2022-01-02 20:49:50Z],
          s3_size: 197
        })

      load_3 =
        Repo.insert!(%CubicLoad{
          table_id: table.id,
          status: "erroring",
          s3_key: "cubic/dmap/sample/20220103.csv.gz",
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197
        })

      load_4 =
        Repo.insert!(%CubicLoad{
          table_id: table.id,
          status: "errored_unknown",
          s3_key: "cubic/dmap/sample/20220104.csv.gz",
          s3_modified: ~U[2022-01-02 20:49:50Z],
          s3_size: 197
        })

      load_5 =
        Repo.insert!(%CubicLoad{
          table_id: table.id,
          status: "ready",
          s3_key: "cubic/dmap/sample/20220105.csv.gz",
          s3_modified: ~U[2022-01-03 20:49:50Z],
          s3_size: 197
        })

      assert :ok = ReleaseTasks.RetryArchiveError.run()

      assert_enqueued(worker: Archive, args: %{load_rec_id: load_1.id})
      assert_enqueued(worker: Archive, args: %{load_rec_id: load_2.id})
      assert_enqueued(worker: Error, args: %{load_rec_id: load_3.id})
      assert_enqueued(worker: Error, args: %{load_rec_id: load_4.id})

      refute_enqueued(worker: Archive, args: %{load_rec_id: load_5.id})
      refute_enqueued(worker: Error, args: %{load_rec_id: load_5.id})
    end
  end
end
