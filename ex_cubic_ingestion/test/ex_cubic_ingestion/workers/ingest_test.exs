defmodule ExCubicIngestion.Workers.IngestTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  import ExCubicIngestion.TestFixtures, only: [setup_tables_loads: 1]

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Workers.Ingest

  require MockExAws

  setup :setup_tables_loads

  describe "perform/1" do
    test "job successfully finished", %{
      dmap_load: dmap_load,
      ods_load: ods_load
    } do
      assert :ok =
               perform_job(Ingest, %{
                 load_rec_ids: [dmap_load.id, ods_load.id],
                 lib_ex_aws: "MockExAws"
               })

      assert ["ready_for_archiving", "ready_for_archiving"] == [
               CubicLoad.get!(dmap_load.id).status,
               CubicLoad.get!(ods_load.id).status
             ]
    end
  end

  describe "construct_glue_job_payload/1" do
    test "payload is contructed correctly with ods and dmap data", %{
      dmap_load: dmap_load,
      ods_load: ods_load
    } do
      {_actual_env, actual_input} =
        Ingest.construct_glue_job_payload([
          dmap_load.id,
          ods_load.id
        ])

      actual_input_decoded = Jason.decode!(actual_input)

      expected_input = %{
        "loads" => [
          %{
            "id" => dmap_load.id,
            "s3_key" => "cubic/dmap/sample/20220101.csv",
            "table_name" => "cubic_dmap__sample",
            "partition_columns" => [
              %{"name" => "identifier", "value" => "20220101.csv"}
            ]
          },
          %{
            "id" => ods_load.id,
            "s3_key" => "cubic/ods_qlik/SAMPLE/LOAD1.csv",
            "table_name" => "cubic_ods_qlik__sample",
            "partition_columns" => [
              %{
                "name" => "snapshot",
                "value" => "20220101T204950Z"
              },
              %{"name" => "identifier", "value" => "LOAD1.csv"}
            ]
          }
        ]
      }

      assert expected_input["loads"] ==
               Enum.sort_by(actual_input_decoded["loads"], & &1["id"])
    end
  end

  describe "monitor_glue_job_run/3" do
    test "monitoring a successful run", %{
      dmap_load: dmap_load,
      ods_load: ods_load
    } do
      assert :ok =
               Ingest.monitor_glue_job_run(
                 "success_run_id",
                 [dmap_load.id, ods_load.id],
                 MockExAws
               )
    end

    test "monitoring a error run", %{
      dmap_load: dmap_load,
      ods_load: ods_load
    } do
      assert {:error, _message} =
               Ingest.monitor_glue_job_run("error_run_id", [dmap_load.id, ods_load.id], MockExAws)
    end
  end

  describe "handle_start_glue_job_error/1" do
    test "receiving a max concurrency exceeded error" do
      assert {:snooze, 60} =
               Ingest.handle_start_glue_job_error(
                 {:error, {"ConcurrentRunsExceededException", "An error occurred."}}
               )
    end

    test "receiving a throttling error" do
      assert {:snooze, 60} =
               Ingest.handle_start_glue_job_error(
                 {:error, {"ThrottlingException", "An error occurred."}}
               )
    end

    test "receiving any other error" do
      assert {:error, _oban_message} =
               Ingest.handle_start_glue_job_error(
                 {:error, {"OtherException", "An error occurred."}}
               )
    end
  end
end
