defmodule ExCubicIngestion.Workers.ArchiveTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Workers.Archive

  require MockExAws

  describe "perform/1" do
    test "run job without error" do
      dmap_table =
        Repo.insert!(%CubicTable{
          name: "cubic_dmap__sample",
          s3_prefix: "cubic/dmap/sample/"
        })

      dmap_load_1 =
        Repo.insert!(%CubicLoad{
          table_id: dmap_table.id,
          status: "ready_for_archiving",
          s3_key: "cubic/dmap/sample/20220101.csv",
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197
        })

      assert :ok ==
               perform_job(Archive, %{
                 load_rec_id: dmap_load_1.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "archived" == CubicLoad.get!(dmap_load_1.id).status
    end
  end
end
