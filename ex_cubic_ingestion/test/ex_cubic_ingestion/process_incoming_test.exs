defmodule ExCubicIngestion.ProcessIncomingTest do
  use ExCubicIngestion.DataCase, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.ProcessIncoming
  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable

  setup do
    {:ok, state, _timeout} = ProcessIncoming.init(lib_ex_aws: MockExAws)

    {:ok, state: state}
  end

  describe "status/0" do
    test "running state" do
      server = start_supervised!({ProcessIncoming, lib_ex_aws: MockExAws})

      assert ProcessIncoming.status(server) == :running
    end
  end

  describe "run/1" do
    test "does nothing without configured tables", %{state: state} do
      :ok = ProcessIncoming.run(state)

      assert Repo.all(CubicLoad) == []
    end

    test "with configured tables, scans the prefixes for files", %{state: state} do
      # insert tables
      dmap_table =
        Repo.insert!(%CubicTable{
          name: "cubic_dmap__sample",
          s3_prefix: "cubic/dmap/sample/",
          is_active: true
        })

      dmap_table_id = dmap_table.id

      ods_table =
        Repo.insert!(%CubicTable{
          name: "cubic_ods_qlik__sample",
          s3_prefix: "cubic/ods_qlik/SAMPLE/",
          is_active: true
        })

      Repo.insert!(%CubicOdsTableSnapshot{
        table_id: ods_table.id,
        snapshot_s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv.gz"
      })

      ods_table_id = ods_table.id

      :ok = ProcessIncoming.run(state)

      [ods_load_1, ods_load_2, ods_load_ct_1, ods_load_ct_2, dmap_load_1, dmap_load_2] =
        Enum.sort_by(Repo.all(CubicLoad), & &1.id)

      assert %CubicLoad{
               s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv.gz",
               status: "ready",
               table_id: ^ods_table_id
             } = ods_load_1

      assert %CubicLoad{
               s3_key: "cubic/ods_qlik/SAMPLE/LOAD2.csv.gz",
               status: "ready",
               table_id: ^ods_table_id
             } = ods_load_2

      assert %CubicLoad{
               s3_key: "cubic/ods_qlik/SAMPLE__ct/20211201-112233444.csv.gz",
               status: "ready",
               table_id: ^ods_table_id
             } = ods_load_ct_1

      assert %CubicLoad{
               s3_key: "cubic/ods_qlik/SAMPLE__ct/20211201-122433444.csv.gz",
               status: "ready",
               table_id: ^ods_table_id
             } = ods_load_ct_2

      assert %CubicLoad{
               s3_key: "cubic/dmap/sample/20220101.csv.gz",
               status: "ready",
               table_id: ^dmap_table_id
             } = dmap_load_1

      assert %CubicLoad{
               s3_key: "cubic/dmap/sample/20220102.csv.gz",
               status: "ready",
               table_id: ^dmap_table_id
             } = dmap_load_2
    end
  end

  describe "prefixes_list/3" do
    test "getting list of prefixes", %{state: state} do
      incoming_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_incoming)
      incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

      prefixes_list = ProcessIncoming.prefixes_list(state, incoming_bucket, incoming_prefix)

      assert ^prefixes_list = [
               %{
                 prefix: "#{incoming_prefix}cubic/ods_qlik/SAMPLE/"
               },
               %{
                 prefix: "#{incoming_prefix}cubic/ods_qlik/SAMPLE__ct/"
               },
               %{
                 prefix: "#{incoming_prefix}cubic/dmap/sample/"
               }
             ]
    end
  end
end
