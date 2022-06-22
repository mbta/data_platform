defmodule ExCubicIngestion.Schema.CubicLoadTest do
  use ExCubicIngestion.DataCase, async: true

  import Ecto.Changeset

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicTable

  setup do
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    cubic_sample = "#{incoming_prefix}cubic/dmap/sample/"

    # insert tables
    table =
      Repo.insert!(%CubicTable{
        name: "cubic_dmap__sample",
        s3_prefix: "cubic/dmap/sample/"
      })

    # only working with dmap loads as distinction doesn't matter in tests
    {:ok,
     %{
       table: table,
       load_objects: MockExAws.Data.load_objects_without_bucket_prefix(cubic_sample)
     }}
  end

  describe "insert_new_from_objects_with_table/1" do
    test "providing a non-empty list of objects", %{
      table: table,
      load_objects: load_objects
    } do
      {:ok, new_load_recs} = CubicLoad.insert_new_from_objects_with_table(load_objects, table)

      assert Enum.map(load_objects, & &1[:key]) ==
               Enum.map(new_load_recs, & &1.s3_key)
    end

    test "providing an empty list of objects", %{
      table: table
    } do
      assert {:ok, []} == CubicLoad.insert_new_from_objects_with_table([], table)
    end
  end

  describe "insert_from_object_with_table/2" do
    test "insert as 'ready' because it's a typical object", %{
      table: table
    } do
      object = %{
        e_tag: "\"ghi123\"",
        key: "cubic/dmap/sample/20220101.csv",
        last_modified: "2022-01-01T20:49:50.000Z",
        owner: nil,
        size: "123",
        storage_class: "STANDARD"
      }

      assert "ready" == CubicLoad.insert_from_object_with_table(object, table).status
    end

    test "insert as 'ready_for_erroring' because of size 0", %{
      table: table
    } do
      object = %{
        e_tag: "\"ghi123\"",
        key: "cubic/dmap/sample/20220101.csv",
        last_modified: "2022-01-01T20:49:50.000Z",
        owner: nil,
        size: "0",
        storage_class: "STANDARD"
      }

      assert "ready_for_erroring" == CubicLoad.insert_from_object_with_table(object, table).status
    end
  end

  describe "get_by_objects/1" do
    test "getting records just added by providing the list we added from", %{
      table: table,
      load_objects: load_objects
    } do
      {:ok, new_load_recs} = CubicLoad.insert_new_from_objects_with_table(load_objects, table)

      assert new_load_recs == CubicLoad.get_by_objects(load_objects)
    end

    test "getting no records by providing a list with a load object not in db", %{
      table: table,
      load_objects: load_objects
    } do
      # put some records in DB
      CubicLoad.insert_new_from_objects_with_table(load_objects, table)

      assert [] ==
               CubicLoad.get_by_objects([
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
      assert [] == CubicLoad.get_by_objects([])
    end
  end

  describe "not_added/2" do
    test "object NOT found in database records", %{
      load_objects: load_objects
    } do
      load_object = List.first(load_objects)

      load_recs = [
        %CubicLoad{
          s3_key: "key/not/found.csv",
          s3_modified: ~U[2022-02-08 20:49:50Z]
        }
      ]

      assert CubicLoad.not_added(load_object, load_recs)
    end

    test "object found in database records", %{
      load_objects: load_objects
    } do
      load_object = List.first(load_objects)

      load_recs = [
        %CubicLoad{
          s3_key: "cubic/dmap/sample/20220101.csv",
          s3_modified: ~U[2022-01-01 20:49:50Z]
        }
      ]

      refute CubicLoad.not_added(load_object, load_recs)
    end
  end

  describe "get_status_ready/0" do
    test "getting load records with the status 'ready'", %{
      table: table,
      load_objects: load_objects
    } do
      # insert records as ready
      {:ok, [first_new_load_rec | rest_new_load_recs]} =
        CubicLoad.insert_new_from_objects_with_table(load_objects, table)

      # set the first record to 'archived'
      Repo.update!(change(first_new_load_rec, status: "archived"))

      # assert that the last record inserted comes back
      assert rest_new_load_recs == CubicLoad.get_status_ready()
    end
  end

  describe "all_by_status_in/1" do
    test "empty list of statuses" do
      assert [] == CubicLoad.all_by_status_in([])
    end

    test "get records by a list of statuses", %{
      table: table
    } do
      # insert loads
      load_1 =
        Repo.insert!(%CubicLoad{
          table_id: table.id,
          status: "ready_for_archiving",
          s3_key: "cubic/dmap/sample/20220101.csv",
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197
        })

      load_2 =
        Repo.insert!(%CubicLoad{
          table_id: table.id,
          status: "ready_for_erroring",
          s3_key: "cubic/dmap/sample/20220102.csv",
          s3_modified: ~U[2022-01-02 20:49:50Z],
          s3_size: 197
        })

      actual_loads = CubicLoad.all_by_status_in(["ready_for_archiving", "ready_for_erroring"])

      assert [load_1.id, load_2.id] ==
               Enum.sort(Enum.map(actual_loads, & &1.id))
    end
  end

  describe "update/2" do
    test "setting an 'archived' status", %{
      table: table,
      load_objects: load_objects
    } do
      # insert records as ready
      {:ok, new_load_recs} = CubicLoad.insert_new_from_objects_with_table(load_objects, table)

      # use the first record
      first_load_rec = List.first(new_load_recs)

      # update it to 'archived' status
      updated_load_rec = CubicLoad.update(first_load_rec, status: "archived")

      assert CubicLoad.get!(first_load_rec.id) == updated_load_rec
    end
  end

  describe "get_many_with_table/1" do
    test "getting no records by passing empty list" do
      assert [] == CubicLoad.get_many_with_table([])
    end

    test "getting records by passing load record IDs", %{
      table: table,
      load_objects: load_objects
    } do
      # insert records as ready
      {:ok, new_load_recs} = CubicLoad.insert_new_from_objects_with_table(load_objects, table)

      new_load_rec_ids = Enum.map(new_load_recs, & &1.id)

      # sort actual recs, as sometimes they might come back out of order
      assert Enum.map(new_load_recs, &{&1, table}) ==
               Enum.sort_by(
                 CubicLoad.get_many_with_table(new_load_rec_ids),
                 fn {load, _table} -> load.id end
               )
    end
  end

  describe "update_many/2" do
    test "updating status to 'ready' for many IDs", %{
      table: table,
      load_objects: load_objects
    } do
      {:ok, new_load_recs} = CubicLoad.insert_new_from_objects_with_table(load_objects, table)

      new_load_rec_ids = Enum.map(new_load_recs, & &1.id)

      CubicLoad.update_many(new_load_rec_ids, status: "ready")

      # assert all are in 'ready' status
      assert Enum.map(new_load_rec_ids, fn _rec_id -> "ready" end) ==
               Enum.map(new_load_rec_ids, &CubicLoad.get!(&1).status)
    end
  end
end
