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

  describe "construct_job_payload/1" do
    test "payload is contructed correctly with ods and dmap data", %{
      dmap_table: dmap_table,
      ods_table: ods_table,
      dmap_load: dmap_load,
      ods_load: ods_load
    } do
      incoming_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_incoming)
      springboard_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_springboard)
      incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

      springboard_prefix =
        Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_springboard)

      {_actual_env, actual_input} =
        Ingest.construct_job_payload([
          dmap_load.id,
          ods_load.id
        ])

      expected_input = %{
        loads: [
          %{
            id: dmap_load.id,
            partition_columns: [
              %{name: "identifier", value: "20220101.csv.gz"}
            ],
            s3_key: "cubic/dmap/sample/20220101.csv.gz",
            destination_path: "s3a://#{incoming_bucket}/#{springboard_prefix}cubic/dmap/sample",
            destination_table_name: "#{dmap_table.name}",
            source_s3_key: "s3://#{incoming_bucket}/#{incoming_prefix}#{dmap_load.s3_key}",
            source_table_name: "#{dmap_table.name}"
          },
          %{
            id: ods_load.id,
            partition_columns: [
              %{
                name: "snapshot",
                value: "20220101T204950Z"
              },
              %{name: "identifier", value: "LOAD1.csv.gz"}
            ],
            s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv.gz",
            destination_path:
              "s3a://#{springboard_bucket}/#{springboard_prefix}raw/cubic/ods_qlik/SAMPLE",
            destination_table_name: "raw_#{ods_table.name}",
            source_s3_key: "s3://#{incoming_bucket}/#{incoming_prefix}#{ods_load.s3_key}",
            source_table_name: "#{ods_table.name}"
          }
        ]
      }

      assert expected_input[:loads] ==
               Enum.sort_by(actual_input[:loads], & &1[:id])
    end
  end

  describe "monitor_glue_job_run/2" do
    test "monitoring a successful run" do
      assert :ok =
               Ingest.monitor_glue_job_run(
                 MockExAws,
                 "success_run_id"
               )
    end

    test "monitoring a error run" do
      assert {:error, _message} = Ingest.monitor_glue_job_run(MockExAws, "error_run_id")
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
