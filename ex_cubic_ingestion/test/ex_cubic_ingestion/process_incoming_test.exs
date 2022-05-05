defmodule ExCubicIngestion.ProcessIncomingTest do
  use ExCubicIngestion.DataCase

  alias ExCubicIngestion.ProcessIncoming
  alias ExCubicIngestion.Schema.CubicLoad

  require MockExAws
  require MockExAws.Data

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

    test "with a configured table, scans the prefixes for files", %{state: state} do
      table = Repo.insert!(MockExAws.Data.table())
      table_id = table.id

      :ok = ProcessIncoming.run(state)

      assert [load1, load2] = Repo.all(CubicLoad)

      assert %CubicLoad{
               s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv",
               status: "ready",
               table_id: ^table_id
             } = load1

      assert %CubicLoad{
               s3_key: "cubic/ods_qlik/SAMPLE/LOAD2.csv",
               status: "ready",
               table_id: ^table_id
             } = load2
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
                 prefix: "#{incoming_prefix}cubic/dmap/agg_sample/"
               }
             ]
    end
  end
end
