defmodule ExCubicOdsIngestion.Schema.CubicOdsLoadTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  setup do
    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)
  end

  describe "insert" do
    # test "from objects" do
    #   CubicOdsLoad.insert_from_objects(MockExAws.Data.get())

    #   new_load_recs = Enum.map(MockExAws.Data.get(), &CubicOdsLoad.insert_ready(&1))

    #   Repo.transaction(fn -> new_load_recs end)

    #   assert new_load_recs ==
    #            ProcessIncoming.load_recs_list(List.first(MockExAws.Data.get()))
    # end

    test "from objects as ready" do
      new_load_recs = Enum.map(MockExAws.Data.get(), &CubicOdsLoad.insert_ready(&1))

      CubicOdsLoad.insert_from_objects(MockExAws.Data.get())

      assert [
               %{
                 status: "ready",
                 s3_key: "vendor/SAMPLE/LOAD1.csv",
                 s3_modified: ~U[2022-02-08 20:49:50Z],
                 s3_size: 197
               },
               %{
                 status: "ready",
                 s3_key: "vendor/SAMPLE/LOAD2.csv",
                 s3_modified: ~U[2022-02-08 20:49:50Z],
                 s3_size: 123
               }
             ] ==
               Enum.map(new_load_recs, fn new_load_rec ->
                 %{
                   status: new_load_rec.status,
                   s3_key: new_load_rec.s3_key,
                   s3_modified: new_load_rec.s3_modified,
                   s3_size: new_load_rec.s3_size
                 }
               end)
    end
  end
end
