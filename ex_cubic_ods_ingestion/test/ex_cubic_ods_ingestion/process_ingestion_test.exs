defmodule ExCubicOdsIngestion.ProcessIngestionTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.ProcessIngestion
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  require MockExAws.Data
  require Logger

  # setup server for each test
  setup do
    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)
  end

  describe "error/1" do
    test "processing error in ingestion" do
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())

      first_load_rec = List.first(new_load_recs)
      updated_load_rec = ProcessIngestion.error(first_load_rec)

      assert "errored" == updated_load_rec.status
    end
  end

  describe "archive/1" do
    test "archiving load after ingestion" do
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())

      first_load_rec = List.first(new_load_recs)
      updated_load_rec = ProcessIngestion.archive(first_load_rec)

      assert "archived" == updated_load_rec.status
    end
  end
end
