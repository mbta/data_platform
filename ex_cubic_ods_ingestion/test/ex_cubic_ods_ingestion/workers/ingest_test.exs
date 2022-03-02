defmodule ExCubicOdsIngestion.Workers.IngestTest do
  use ExUnit.Case
  use Oban.Testing, repo: ExCubicOdsIngestion.Repo

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Schema.CubicOdsTable
  alias ExCubicOdsIngestion.StartIngestion
  alias ExCubicOdsIngestion.Workers.Ingest

  require MockExAws

  setup do
    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)
  end

  describe "perform/1" do
    test "enqueueing job" do
      # @todo the assertion below is not working
      # assert_enqueued(
      #   worker: Ingest,
      #   args: %{
      #     load_rec_ids: [123,456]
      #   }
      # )
    end

    test "run job" do
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
      last_load_rec = List.last(new_load_recs)

      # attach tables
      StartIngestion.attach_table(first_load_rec)
      StartIngestion.attach_table(last_load_rec)

      assert :ok =
               perform_job(Ingest, %{
                 load_rec_ids: [first_load_rec.id, last_load_rec.id],
                 lib_ex_aws: "MockExAws"
               })
    end
  end
end
