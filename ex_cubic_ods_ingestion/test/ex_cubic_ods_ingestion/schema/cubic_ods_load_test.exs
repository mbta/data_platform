defmodule ExCubicOdsIngestion.Schema.CubicOdsLoadTest do
  use ExUnit.Case

  import Ecto.Changeset

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

  describe "not_added/2" do
    test "object NOT found in database records" do
      load_object = List.first(MockExAws.Data.load_objects())

      load_recs = [
        %CubicOdsLoad{
          s3_key: "key/not/found.csv",
          s3_modified: ~U[2022-02-08 20:49:50Z]
        }
      ]

      assert CubicOdsLoad.not_added(load_object, load_recs)
    end

    test "object found in database records" do
      load_object = List.first(MockExAws.Data.load_objects())

      load_recs = [
        %CubicOdsLoad{
          s3_key: "vendor/SAMPLE/LOAD1.csv",
          s3_modified: ~U[2022-02-08 20:49:50Z]
        }
      ]

      refute CubicOdsLoad.not_added(load_object, load_recs)
    end
  end

  describe "get_status_ready/0" do
    test "getting load records with the status 'ready'" do
      # insert records as ready
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())
      # set the first record to 'archived'
      {:ok, _archived_load_rec} =
        Repo.transaction(fn ->
          Repo.update!(change(List.first(new_load_recs), status: "archived"))
        end)

      ready_load_recs = CubicOdsLoad.get_status_ready()
      # filter down to the ones we just inserted
      filtered_ready_load_recs = Enum.filter(ready_load_recs, &Enum.member?(new_load_recs, &1))

      # assert that the last record inserted comes back
      assert [List.last(new_load_recs)] == filtered_ready_load_recs
    end
  end

  describe "update/2" do
    test "setting an 'archived' status" do
      # insert records as ready
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.load_objects())

      # use the first record
      first_load_rec = List.first(new_load_recs)

      # update it to 'archived' status
      updated_load_rec = CubicOdsLoad.update(first_load_rec, status: "archived")

      assert Repo.get!(CubicOdsLoad, first_load_rec.id) == updated_load_rec
    end
  end
end
