defmodule ExCubicOdsIngestion.Workers.ArchiveTest do
  use ExCubicOdsIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicOdsIngestion.Repo

  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Workers.Archive

  require MockExAws

  describe "perform/1" do
    test "run job without error" do
      snapshot = DateTime.truncate(DateTime.utc_now(), :second)
      # insert a new table
      new_table_rec = Repo.insert!(%{MockExAws.Data.table() | snapshot: snapshot})

      # insert load record
      first_load_rec =
        Repo.insert!(%CubicOdsLoad{
          status: "ready",
          table_id: new_table_rec.id,
          s3_key: new_table_rec.snapshot_s3_key,
          s3_modified: snapshot,
          snapshot: snapshot,
          s3_size: 0
        })

      assert :ok ==
               perform_job(Archive, %{
                 load_rec_id: first_load_rec.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "archived" == CubicOdsLoad.get!(first_load_rec.id).status
    end

    # test "running with load file that doesn't exist" do
    #   new_table_rec = %CubicOdsTable{
    #     name: "does_not_exist",
    #     s3_prefix: "does_not_exist/",
    #     snapshot_s3_key: "does_not_exist/file.csv"
    #   }

    #   {:ok, _inserted_table_rec} =
    #     Repo.transaction(fn ->
    #       Repo.insert!(new_table_rec)
    #     end)

    #   # insert load records
    #   {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects([
    #   %{
    #     e_tag: "\"abc123\"",
    #     key: "does_not_exist/file.csv",
    #     last_modified: "2022-02-08T20:49:50.000Z",
    #     owner: nil,
    #     size: "197",
    #     storage_class: "STANDARD"
    #   }])
    #   new_load_rec = List.first(new_load_recs)

    #   # attach tables
    #   StartIngestion.attach_table(new_load_rec)

    #   assert :ok ==
    #            perform_job(Archive, %{
    #              load_rec_ids: [new_load_rec.id],
    #              lib_ex_aws: "MockExAws"
    #            })
    # end
  end
end
