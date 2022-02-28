defmodule ExCubicOdsIngestion.Workers.IngestTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Schema.CubicOdsTable
  alias ExCubicOdsIngestion.StartIngestion
  alias ExCubicOdsIngestion.Workers.Ingest

  require Oban.Job
  require MockExAws
  require Logger

  setup do
    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)
  end

  describe "perform/1" do
    test "run job" do
      # insert a new table
      new_table_rec = %CubicOdsTable{
        name: "vendor__sample",
        s3_prefix: "vendor/SAMPLE/",
        snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
      }

      {:ok, inserted_table_rec} =
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

      # prepare loads and get first chunk
      {_prepared_error_loads, prepared_ready_loads_chunks} =
        StartIngestion.prepare_loads(new_load_recs)

      first_prepared_ready_loads_chunk = List.first(prepared_ready_loads_chunks)

      # attach the table and create oban job
      {:ok, oban_job} = StartIngestion.start_ingestion(first_prepared_ready_loads_chunk)

      updated_args_oban_job = %{
        oban_job
        | args: %{
            "chunk" => [
              %{
                "load" => %{"id" => first_load_rec.id},
                "table" => %{"id" => inserted_table_rec.id}
              },
              %{
                "load" => %{"id" => last_load_rec.id},
                "table" => %{"id" => inserted_table_rec.id}
              }
            ],
            "lib_ex_aws" => "MockExAws"
          }
      }

      assert :ok = Ingest.perform(updated_args_oban_job)

      updated_meta_oban_job = %{
        updated_args_oban_job
        | meta: %{
            "glue_job_run_id" => "abc123"
          }
      }

      assert :ok = Ingest.perform(updated_meta_oban_job)
    end
  end
end
