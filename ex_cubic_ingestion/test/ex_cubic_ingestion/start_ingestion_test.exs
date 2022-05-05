defmodule ExCubicIngestion.StartIngestionTest do
  use ExCubicIngestion.DataCase
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.StartIngestion
  alias ExCubicIngestion.Workers.Ingest

  require MockExAws.Data
  require Logger

  # setup server for each test
  setup do
    # insert table
    table = Repo.insert!(MockExAws.Data.table())

    # insert load records
    {:ok, load_recs} =
      CubicLoad.insert_new_from_objects_with_table(
        MockExAws.Data.load_objects_without_bucket_prefix(),
        table
      )

    [first_load_rec, last_load_rec] = load_recs

    {:ok,
     %{
       table: table,
       load_recs: load_recs,
       first_load_rec: first_load_rec,
       last_load_rec: last_load_rec
     }}
  end

  describe "status/0" do
    test "running state" do
      server = start_supervised!(StartIngestion)

      assert StartIngestion.status(server) == :running
    end
  end

  describe "run/0" do
    test "schedules ingestion for ready loads", %{
      load_recs: new_load_recs
    } do
      assert :ok = StartIngestion.run()

      for %{id: load_rec_id} <- new_load_recs,
          load_rec = CubicLoad.get!(load_rec_id) do
        assert load_rec.status == "ingesting"
      end

      load_rec_ids = Enum.map(new_load_recs, & &1.id)

      assert_enqueued(worker: Ingest, args: %{load_rec_ids: load_rec_ids})
    end
  end
end
