defmodule ExCubicOdsIngestion.ProcessIncomingTest do
  use ExCubicOdsIngestion.DataCase

  alias ExCubicOdsIngestion.ProcessIncoming
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  require MockExAws
  require MockExAws.Data

  describe "status/0" do
    test "running state" do
      server = start_supervised!({ProcessIncoming, lib_ex_aws: MockExAws})

      assert ProcessIncoming.status(server) == :running
    end
  end

  describe "run/1" do
    setup do
      {:ok, state, _timeout} = ProcessIncoming.init(lib_ex_aws: MockExAws)

      {:ok, state: state}
    end

    test "does nothing without configured tables", %{state: state} do
      :ok = ProcessIncoming.run(state)

      assert Repo.all(CubicOdsLoad) == []
    end

    test "with a configured table, scans the prefixes for files", %{state: state} do
      table = Repo.insert!(MockExAws.Data.table())
      table_id = table.id

      :ok = ProcessIncoming.run(state)

      assert [load1, load2] = Repo.all(CubicOdsLoad)

      assert %CubicOdsLoad{
               s3_key: "cubic_ods_qlik/SAMPLE/LOAD1.csv",
               status: "ready",
               table_id: ^table_id
             } = load1

      assert %CubicOdsLoad{
               s3_key: "cubic_ods_qlik/SAMPLE/LOAD2.csv",
               status: "ready",
               table_id: ^table_id
             } = load2
    end
  end
end
