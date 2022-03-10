defmodule ExCubicOdsIngestion.Workers.IngestTest do
  use ExCubicOdsIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicOdsIngestion.Repo

  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Workers.Ingest

  require MockExAws

  describe "perform/1" do
    test "run job" do
      # insert a new table
      {:ok, new_table_rec} = Repo.insert(MockExAws.Data.table())

      # insert load records
      {:ok, new_load_recs} =
        CubicOdsLoad.insert_new_from_objects_with_table(
          MockExAws.Data.load_objects(),
          new_table_rec
        )

      first_load_rec = List.first(new_load_recs)
      last_load_rec = List.last(new_load_recs)

      assert :ok =
               perform_job(Ingest, %{
                 load_rec_ids: [first_load_rec.id, last_load_rec.id],
                 lib_ex_aws: "MockExAws"
               })

      assert "ready_for_archiving" == CubicOdsLoad.get!(first_load_rec.id).status
    end
  end
end
