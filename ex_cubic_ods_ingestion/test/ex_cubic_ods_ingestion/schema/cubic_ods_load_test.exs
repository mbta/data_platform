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

  describe "insert_from_objects/1" do
    test "providing a non-empty list of objects" do
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())

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
                 s3_modified: ~U[2022-02-08 20:50:50Z],
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

    test "providing an empty list of objects" do
      assert {:ok, []} == CubicOdsLoad.insert_from_objects([])
    end
  end

  describe "get_by_objects/1" do
    test "getting records just added by providing the list we added from" do
      load_objects = MockExAws.Data.load_objects()
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(load_objects)

      assert new_load_recs ==
               CubicOdsLoad.get_by_objects(load_objects)
    end

    test "getting no records by providing a list with a load object not in db" do
      {:ok, _new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())

      assert [] ==
               CubicOdsLoad.get_by_objects([
                 %{
                   e_tag: "\"ghi789\"",
                   key: "not/in/db.csv",
                   last_modified: "2022-02-08T21:49:50.000Z",
                   owner: nil,
                   size: "197",
                   storage_class: "STANDARD"
                 }
               ])
    end

    test "getting no records by providing an empty list" do
      assert [] == CubicOdsLoad.get_by_objects([])
    end

    # @todo test for improper load object map
  end
end
