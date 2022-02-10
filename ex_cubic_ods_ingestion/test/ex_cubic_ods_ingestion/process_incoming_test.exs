defmodule ExCubicOdsIngestion.ProcessIncomingTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox

  require ExCubicOdsIngestion.Schema.CubicOdsLoad

  # setup server for use throughout tests
  setup do
    # Explicitly get a connection before each test
    :ok = Sandbox.checkout(ExCubicOdsIngestion.Repo)

    # start a supervisor
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
        %{
          e_tag: "\"abc123\"",
          key: "vendor/SAMPLE/LOAD1.csv",
          last_modified: "2022-02-08T20:49:50.000Z",
          owner: nil,
          size: "197",
          storage_class: "STANDARD"
        },
        %{
          e_tag: "\"def123\"",
          key: "vendor/SAMPLE/LOAD2.csv",
          last_modified: "2022-02-08T20:49:50.000Z",
          owner: nil,
          size: "123",
          storage_class: "STANDARD"
        }
      ]

      assert [load_objects, ""] ==
               ExCubicOdsIngestion.ProcessIncoming.load_objects_list("cubic_ods_qlik_test/", "")
    end

    test "get list of load records" do
      load_recs = [
        %ExCubicOdsIngestion.Schema.CubicOdsLoad{
          s3_key: "vendor/SAMPLE/LOAD1.csv"
        },
        %ExCubicOdsIngestion.Schema.CubicOdsLoad{
          s3_key: "vendor/SAMPLE/LOAD2.csv"
        }
      ]

      assert load_recs ==
               ExCubicOdsIngestion.ProcessIncoming.load_recs_list(%{
                 key: "vendor/SAMPLE/LOAD1.csv",
                 last_modified: "2021-02-08T20:49:50.000Z"
               })
    end
  end
end
