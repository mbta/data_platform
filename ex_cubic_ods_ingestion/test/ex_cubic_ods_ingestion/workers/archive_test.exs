defmodule ExCubicOdsIngestion.Workers.ArchiveTest do
  use ExUnit.Case
  use Oban.Testing, repo: ExCubicOdsIngestion.Repo

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Schema.CubicOdsTable
  alias ExCubicOdsIngestion.StartIngestion
  alias ExCubicOdsIngestion.Workers.Archive

  require MockExAws

  setup do
    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)
  end

  describe "perform/1" do
    test "run job without error" do
      # insert a new table
      new_table_rec = %CubicOdsTable{
        name: "vendor__sample",
        s3_prefix: "vendor/SAMPLE/",
        snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
      }

      {:ok, _inserted_table_rec} =
        Repo.transaction(fn ->
          Repo.insert!(new_table_rec)
        end)

      # insert load records
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())
      first_load_rec = List.first(new_load_recs)

      # attach tables
      StartIngestion.attach_table(first_load_rec)

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
