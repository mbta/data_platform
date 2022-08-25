defmodule ExCubicIngestion.Schema.CubicLoadTest do
  use ExCubicIngestion.DataCase, async: true

  import Ecto.Changeset

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable

  setup do
    # insert tables
    table =
      Repo.insert!(%CubicTable{
        name: "cubic_dmap__sample",
        s3_prefix: "cubic/dmap/sample/",
        is_raw: false
      })

    utc_now = DateTime.utc_now()

    load_objects = [
      %{
        e_tag: "\"ghi123\"",
        key: "cubic/dmap/sample/20220101.csv.gz",
        last_modified: MockExAws.Data.dt_adjust_and_format(utc_now, -3600),
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"jkl123\"",
        key: "cubic/dmap/sample/20220102.csv.gz",
        last_modified: MockExAws.Data.dt_adjust_and_format(utc_now, -3000),
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      }
    ]

    # only working with dmap loads as distinction doesn't matter in tests
    {:ok,
     %{
       table: table,
       utc_now: utc_now,
       load_objects: load_objects
     }}
  end

  describe "insert_new_from_objects_with_table/1" do
    test "providing a non-empty list of objects", %{
      table: table,
      utc_now: utc_now,
      load_objects: load_objects
    } do
      {:ok, new_load_recs} = CubicLoad.insert_new_from_objects_with_table(load_objects, table)

      assert Enum.map(load_objects, & &1[:key]) ==
               Enum.map(new_load_recs, & &1.s3_key)

      # inserting again should not return any new records
      assert {:ok, []} == CubicLoad.insert_new_from_objects_with_table(load_objects, table)

      # add a new object
      load_objects = [
        %{
          key: "cubic/dmap/sample/20220103.csv",
          last_modified: MockExAws.Data.dt_adjust_and_format(utc_now, -2400),
          size: "197"
        }
        | load_objects
      ]

      # adding one more load object, should only insert it as a load record
      assert {:ok,
              [
                %CubicLoad{
                  s3_key: "cubic/dmap/sample/20220103.csv"
                }
              ]} = CubicLoad.insert_new_from_objects_with_table(load_objects, table)
    end

    test "providing an empty list of objects", %{
      table: table
    } do
      assert {:ok, []} == CubicLoad.insert_new_from_objects_with_table([], table)
    end
  end

  describe "insert_from_object_with_table/2" do
    test "insert as 'ready' because it's a typical object", %{
      table: table,
      load_objects: load_objects
    } do
      table_id = table.id

      assert %CubicLoad{
               table_id: ^table_id,
               status: "ready",
               s3_key: "cubic/dmap/sample/20220101.csv.gz"
             } = CubicLoad.insert_from_object_with_table(List.first(load_objects), table)
    end

    test "insert as 'ready_for_erroring' because of size 0", %{
      table: table,
      load_objects: load_objects
    } do
      object = List.first(load_objects)

      assert "ready_for_erroring" ==
               CubicLoad.insert_from_object_with_table(%{object | size: "0"}, table).status
    end
  end

  describe "get_status_ready/0" do
    test "getting non-ODS 'ready' loads", %{
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

    test "getting ODS 'ready' loads" do
      # insert ODS table and snapshot
      ods_table =
        Repo.insert!(%CubicTable{
          name: "cubic_ods_qlik__sample",
          s3_prefix: "cubic/ods_qlik/SAMPLE/",
          is_raw: true
        })

      ods_snapshot_s3_key = "cubic/ods_qlik/SAMPLE/LOAD1.csv"
      ods_snapshot = ~U[2022-01-01 20:49:50Z]

      Repo.insert!(%CubicOdsTableSnapshot{
        table_id: ods_table.id,
        snapshot: ods_snapshot,
        snapshot_s3_key: ods_snapshot_s3_key
      })

      # insert loads
      ods_load =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "ready",
          s3_key: ods_snapshot_s3_key,
          s3_modified: ods_snapshot,
          s3_size: 197,
          is_raw: true
        })

      Repo.insert!(%CubicLoad{
        table_id: ods_table.id,
        status: "ready",
        s3_key: "cubic/ods_qlik/SAMPLE/LOAD2.csv",
        s3_modified: ~U[2022-01-02 20:49:50Z],
        s3_size: 197,
        is_raw: true
      })

      # only get the first load because of limit
      assert [ods_load] == CubicLoad.get_status_ready(1)

      # add new snapshot load
      new_ods_load =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "ready",
          s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv",
          s3_modified: ~U[2022-01-03 20:49:50Z],
          s3_size: 197,
          is_raw: true
        })

      # ignoring loads prior to this last snapshot load
      assert [new_ods_load] == CubicLoad.get_status_ready()
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
          s3_key: "cubic/dmap/sample/20220101.csv.gz",
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197
        })

      load_2 =
        Repo.insert!(%CubicLoad{
          table_id: table.id,
          status: "ready_for_erroring",
          s3_key: "cubic/dmap/sample/20220102.csv.gz",
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
      updated_load_rec = CubicLoad.update(first_load_rec, %{status: "archived"})

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
