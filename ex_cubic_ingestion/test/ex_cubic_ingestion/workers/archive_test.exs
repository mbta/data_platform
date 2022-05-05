defmodule ExCubicIngestion.Workers.ArchiveTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Workers.Archive

  require MockExAws

  describe "perform/1" do
    test "run job without error" do
      created = DateTime.truncate(DateTime.utc_now(), :second)
      # insert a new table
      new_table_rec = Repo.insert!(MockExAws.Data.table())

      # insert load record
      first_load_rec =
        Repo.insert!(%CubicLoad{
          status: "ready",
          table_id: new_table_rec.id,
          s3_key: "#{new_table_rec.s3_prefix}LOAD1.csv",
          s3_modified: created,
          s3_size: 0
        })

      assert :ok ==
               perform_job(Archive, %{
                 load_rec_id: first_load_rec.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "archived" == CubicLoad.get!(first_load_rec.id).status
    end

    # test "running with load file that doesn't exist" do
    #   new_table_rec = %CubicTable{
    #     name: "does_not_exist",
    #     s3_prefix: "does_not_exist/",
    #   }

    #   {:ok, _inserted_table_rec} =
    #     Repo.transaction(fn ->
    #       Repo.insert!(new_table_rec)
    #     end)

    #   # insert load records
    #   {:ok, new_load_recs} = CubicLoad.insert_new_from_objects([
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
