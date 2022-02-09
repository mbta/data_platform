defmodule ExCubicOdsIngestion.ProcessIncomingTest do
  use ExUnit.Case

  # setup server for use throughout tests
  setup do
    server = start_supervised!(ExCubicOdsIngestion.ProcessIncoming)

    %{server: server}
  end

  describe "genserver" do
    test "run", %{server: server} do
      assert ExCubicOdsIngestion.ProcessIncoming.status(server) == :running
    end
  end

  describe "run" do
    test "get list of load objects" do
      load_objects = [
        %{},
        %{}
      ]

      assert load_objects ==
               ExCubicOdsIngestion.ProcessIncoming.load_objects_list("cubic_ods_qlik_test/", "")
    end

    test "get list of load records" do
      load_records = [
        %{},
        %{}
      ]

      assert load_records ==
               ExCubicOdsIngestion.ProcessIncoming.load_recs_list(%{
                 key: "gg/incoming/cubic_ods_qlik_test/EDW.SAMPLE/LOAD1.csv",
                 last_modified: "2022-02-08T20:49:50.000Z"
               })
    end
  end
end
