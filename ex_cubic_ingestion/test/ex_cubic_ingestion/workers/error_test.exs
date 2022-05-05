defmodule ExCubicIngestion.Workers.ErrorTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Workers.Error

  require MockExAws

  describe "perform/1" do
    test "run job without error" do
      # insert a new table
      new_table_rec = Repo.insert!(MockExAws.Data.table())

      # insert load records
      {:ok, new_load_recs} =
        CubicLoad.insert_new_from_objects_with_table(
          MockExAws.Data.load_objects_without_bucket_prefix(),
          new_table_rec
        )

      first_load_rec = List.first(new_load_recs)

      assert :ok ==
               perform_job(Error, %{
                 load_rec_id: first_load_rec.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "errored" == CubicLoad.get!(first_load_rec.id).status
    end
  end
end
