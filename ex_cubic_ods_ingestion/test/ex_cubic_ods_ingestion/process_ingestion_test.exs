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

  describe "archive/1" do
    test "archiving load after ingestion" do
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())

      updated_load_recs =
        ProcessIngestion.archive(
          Enum.map(new_load_recs, fn new_load_rec -> %{"load" => %{"id" => new_load_rec.id}} end)
        )

      assert {Enum.count(new_load_recs),
              Enum.map(new_load_recs, fn new_load_rec -> new_load_rec.id end)} ==
               updated_load_recs
    end
  end

  describe "error/1" do
    test "processing error in ingestion" do
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())

      updated_load_recs =
        ProcessIngestion.error(
          Enum.map(new_load_recs, fn new_load_rec -> %{"load" => %{"id" => new_load_rec.id}} end)
        )

      assert {Enum.count(new_load_recs),
              Enum.map(new_load_recs, fn new_load_rec -> new_load_rec.id end)} ==
               updated_load_recs
    end
  end
end
