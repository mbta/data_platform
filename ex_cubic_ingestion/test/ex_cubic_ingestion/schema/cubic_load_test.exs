defmodule ExCubicIngestion.Schema.CubicLoadTest do
  use ExCubicIngestion.DataCase, async: true

  import Ecto.Changeset

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable

  setup do
    utc_now = DateTime.utc_now()

    dmap_table =
      Repo.insert!(%CubicTable{
        name: "cubic_dmap__sample",
        s3_prefix: "cubic/dmap/sample/",
        is_raw: false
      })

    dmap_load_objects = [
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

    ods_table =
      Repo.insert!(%CubicTable{
        name: "cubic_ods_qlik__sample",
        s3_prefix: "cubic/ods_qlik/SAMPLE/",
        is_raw: true
      })

    ods_table_snapshot =
      Repo.insert!(%CubicOdsTableSnapshot{
        table_id: ods_table.id,
        snapshot_s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv.gz"
      })

    ods_load_objects = [
      %{
        e_tag: "\"abc123\"",
        key: "cubic/ods_qlik/SAMPLE/LOAD1.csv.gz",
        last_modified: MockExAws.Data.dt_adjust_and_format(utc_now, -3600),
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"def123\"",
        key: "cubic/ods_qlik/SAMPLE/LOAD2.csv.gz",
        last_modified: MockExAws.Data.dt_adjust_and_format(utc_now, -3000),
        owner: nil,
        size: "123",
        storage_class: "STANDARD"
      }
    ]

    {:ok,
     %{
       utc_now: utc_now,
       dmap_table: dmap_table,
       dmap_load_objects: dmap_load_objects,
       ods_table: ods_table,
       ods_table_snapshot: ods_table_snapshot,
       ods_load_objects: ods_load_objects
     }}
  end

  describe "insert_new_from_objects_with_table/1" do
    test "providing a non-empty list of DMAP objects", %{
      utc_now: utc_now,
      dmap_table: dmap_table,
      dmap_load_objects: dmap_load_objects
    } do
      {:ok, {nil, new_dmap_load_recs}} =
        CubicLoad.insert_new_from_objects_with_table(dmap_load_objects, dmap_table)

      assert Enum.map(dmap_load_objects, & &1.key) ==
               new_dmap_load_recs
               |> Enum.reverse()
               |> Enum.map(fn {%CubicLoad{status: "ready"} = load, nil, _table, nil} ->
                 load.s3_key
               end)

      assert {:ok, {nil, []}} ==
               CubicLoad.insert_new_from_objects_with_table(dmap_load_objects, dmap_table)

      # add a new object
      dmap_load_objects = [
        %{
          key: "cubic/dmap/sample/20220103.csv.gz",
          last_modified: MockExAws.Data.dt_adjust_and_format(utc_now, -2400),
          size: "197"
        }
        | dmap_load_objects
      ]

      # adding one more load object, should only insert it as a load record
      assert {:ok,
              {nil, [{%CubicLoad{s3_key: "cubic/dmap/sample/20220103.csv.gz"}, nil, _table, nil}]}} =
               CubicLoad.insert_new_from_objects_with_table(dmap_load_objects, dmap_table)
    end

    test "providing a non-empty list of ODS objects", %{
      utc_now: utc_now,
      ods_table: ods_table,
      ods_load_objects: ods_load_objects
    } do
      {:ok, {_last_ods_table_snapshot, new_ods_load_recs}} =
        CubicLoad.insert_new_from_objects_with_table(ods_load_objects, ods_table)

      assert Enum.map(ods_load_objects, & &1.key) ==
               new_ods_load_recs
               |> Enum.reverse()
               |> Enum.map(fn {%CubicLoad{status: "ready", s3_key: s3_key}, _ods_load_snapshot,
                               _table, _ods_table_snapshot} ->
                 s3_key
               end)

      # inserting again should not return any new records
      assert {:ok, {_last_ods_table_snapshot, []}} =
               CubicLoad.insert_new_from_objects_with_table(ods_load_objects, ods_table)

      # add a new object
      ods_load_objects = [
        %{
          key: "cubic/ods_qlik/SAMPLE/LOAD3.csv.gz",
          last_modified: MockExAws.Data.dt_adjust_and_format(utc_now, -2400),
          size: "197"
        }
        | ods_load_objects
      ]

      # adding one more load object, should only insert it as a load record
      assert {:ok,
              {_last_ods_table_snapshot,
               [
                 {%CubicLoad{s3_key: "cubic/ods_qlik/SAMPLE/LOAD3.csv.gz"}, _ods_load_snapshot,
                  _table, _ods_table_snapshot}
               ]}} = CubicLoad.insert_new_from_objects_with_table(ods_load_objects, ods_table)
    end

    test "providing an empty list of objects", %{
      dmap_table: dmap_table
    } do
      assert {:ok, {nil, []}} = CubicLoad.insert_new_from_objects_with_table([], dmap_table)
    end
  end

  describe "insert_from_object_with_table/2" do
    test "inserting DMAP objects", %{
      dmap_table: dmap_table,
      dmap_load_objects: dmap_load_objects
    } do
      first_load_object = List.first(dmap_load_objects)

      assert {%CubicLoad{status: "ready"}, nil, %CubicTable{}, nil} =
               CubicLoad.insert_from_object_with_table(first_load_object, {dmap_table, nil})

      assert {%CubicLoad{status: "ready_for_erroring"}, nil, %CubicTable{}, nil} =
               CubicLoad.insert_from_object_with_table(
                 %{first_load_object | size: "0"},
                 {dmap_table, nil}
               )
    end

    test "inserting ODS objects", %{
      ods_table: ods_table,
      ods_table_snapshot: ods_table_snapshot,
      ods_load_objects: ods_load_objects
    } do
      ods_table_id = ods_table.id
      ods_table_snapshot_id = ods_table_snapshot.id

      snapshot_load_object = List.first(ods_load_objects)
      second_load_object = List.last(ods_load_objects)

      # insert the second load with an empty ODS table snapshot value
      assert {%CubicLoad{status: "ready_for_erroring"}, %CubicOdsLoadSnapshot{snapshot: nil},
              %CubicTable{id: ^ods_table_id},
              %CubicOdsTableSnapshot{id: ^ods_table_snapshot_id}} =
               CubicLoad.insert_from_object_with_table(
                 second_load_object,
                 {ods_table, ods_table_snapshot}
               )

      snapshot = CubicLoad.parse_and_drop_msec(snapshot_load_object.last_modified)

      # inserting a snapshot load should update the table's snapshot
      assert {%CubicLoad{status: "ready"}, %CubicOdsLoadSnapshot{snapshot: ^snapshot},
              %CubicTable{id: ^ods_table_id},
              %CubicOdsTableSnapshot{id: ^ods_table_snapshot_id, snapshot: ^snapshot} =
                updated_ods_table_snapshot} =
               CubicLoad.insert_from_object_with_table(
                 snapshot_load_object,
                 {ods_table, ods_table_snapshot}
               )

      # insert the second load, but with an updated ODS table snapshot record
      assert {%CubicLoad{status: "ready"}, %CubicOdsLoadSnapshot{snapshot: ^snapshot},
              %CubicTable{id: ^ods_table_id},
              %CubicOdsTableSnapshot{id: ^ods_table_snapshot_id, snapshot: ^snapshot}} =
               CubicLoad.insert_from_object_with_table(
                 second_load_object,
                 {ods_table, updated_ods_table_snapshot}
               )

      # lastly, test out load with size of 0
      assert {%CubicLoad{status: "ready_for_erroring"},
              %CubicOdsLoadSnapshot{snapshot: ^snapshot}, %CubicTable{id: ^ods_table_id},
              %CubicOdsTableSnapshot{id: ^ods_table_snapshot_id, snapshot: ^snapshot}} =
               CubicLoad.insert_from_object_with_table(
                 %{second_load_object | size: "0"},
                 {ods_table, updated_ods_table_snapshot}
               )
    end
  end

  describe "get_status_ready/0" do
    test "getting only 'ready' loads after updating one to 'archived'", %{
      dmap_table: dmap_table,
      dmap_load_objects: dmap_load_objects
    } do
      # insert records as ready
      {:ok, {nil, [{first_new_load_rec, nil, _table, nil} | rest_new_load_recs]}} =
        CubicLoad.insert_new_from_objects_with_table(dmap_load_objects, dmap_table)

      # set the first record to 'archived'
      Repo.update!(change(first_new_load_rec, status: "archived"))

      # assert that the last record inserted comes back
      assert Enum.map(rest_new_load_recs, fn {load, _ods_load_snapshot, table,
                                              _ods_table_snapshot} ->
               {load, table}
             end) == CubicLoad.get_status_ready()
    end
  end

  describe "all_by_status_in/2" do
    test "empty list of statuses" do
      assert [] = CubicLoad.all_by_status_in([])
    end

    test "get records by a list of statuses", %{
      dmap_table: dmap_table
    } do
      # insert loads
      load_1 =
        Repo.insert!(%CubicLoad{
          table_id: dmap_table.id,
          status: "ready_for_archiving",
          s3_key: "cubic/dmap/sample/20220101.csv.gz",
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197
        })

      load_2 =
        Repo.insert!(%CubicLoad{
          table_id: dmap_table.id,
          status: "ready_for_erroring",
          s3_key: "cubic/dmap/sample/20220102.csv.gz",
          s3_modified: ~U[2022-01-02 20:49:50Z],
          s3_size: 197
        })

      assert [load_1, load_2] ==
               ["ready_for_archiving", "ready_for_erroring"]
               |> CubicLoad.all_by_status_in()
               |> Enum.sort_by(& &1.id)
    end

    test "limiting the number of records returned", %{
      dmap_table: dmap_table,
      dmap_load_objects: dmap_load_objects
    } do
      CubicLoad.insert_new_from_objects_with_table(dmap_load_objects, dmap_table)

      assert 1 == length(CubicLoad.all_by_status_in(["ready"], 1))
    end
  end

  describe "update/2" do
    test "setting an 'archived' status", %{
      dmap_table: dmap_table,
      dmap_load_objects: dmap_load_objects
    } do
      # insert records as ready
      {:ok, {nil, [{first_new_load_rec, nil, _table, nil} | _rest]}} =
        CubicLoad.insert_new_from_objects_with_table(dmap_load_objects, dmap_table)

      # update it to 'archived' status
      updated_load_rec = CubicLoad.update(first_new_load_rec, %{status: "archived"})

      assert CubicLoad.get!(first_new_load_rec.id) == updated_load_rec
    end
  end

  describe "get_many_with_table/1" do
    test "getting no records by passing empty list" do
      assert [] == CubicLoad.get_many_with_table([])
    end

    test "getting records by passing load record IDs", %{
      dmap_table: dmap_table,
      dmap_load_objects: dmap_load_objects
    } do
      # insert records as ready
      {:ok, {nil, new_load_recs}} =
        CubicLoad.insert_new_from_objects_with_table(dmap_load_objects, dmap_table)

      expected =
        new_load_recs
        |> Enum.map(fn {load, _ods_load_snapshot, table, _ods_table_snapshot} -> {load, table} end)
        |> Enum.reverse()

      actual =
        new_load_recs
        |> Enum.map(fn {load, _ods_load_snapshot, _table, _ods_table_snapshot} -> load.id end)
        |> CubicLoad.get_many_with_table()
        |> Enum.sort_by(fn {load, _table} -> load.id end)

      # sort actual recs, as sometimes they might come back out of order
      assert expected == actual
    end
  end

  describe "update_many/2" do
    test "updating status to 'ready_for_erroring' for many IDs", %{
      dmap_table: dmap_table,
      dmap_load_objects: dmap_load_objects
    } do
      # insert records as ready
      {:ok, {nil, new_load_recs}} =
        CubicLoad.insert_new_from_objects_with_table(dmap_load_objects, dmap_table)

      new_load_rec_ids =
        Enum.map(new_load_recs, fn {load, _ods_load_snapshot, _table, _ods_table_snapshot} ->
          load.id
        end)

      # update all to 'ready_for_erroring'
      CubicLoad.update_many(new_load_rec_ids, status: "ready_for_erroring")

      # assert all are in 'ready_for_erroring' status
      assert Enum.map(new_load_rec_ids, fn _rec_id -> "ready_for_erroring" end) ==
               Enum.map(new_load_rec_ids, &CubicLoad.get!(&1).status)
    end
  end

  describe "get_status_ready_by_table_query/3" do
    test "getting non-ODS 'ready' loads", %{
      dmap_table: dmap_table,
      dmap_load_objects: dmap_load_objects
    } do
      {:ok, {nil, new_load_recs}} =
        CubicLoad.insert_new_from_objects_with_table(dmap_load_objects, dmap_table)

      {_, ready_loads_by_table_query} =
        CubicLoad.get_status_ready_by_table_query({dmap_table, nil})

      expected =
        new_load_recs
        |> Enum.reverse()
        |> Enum.map(fn {load, _ods_load_snapshot, _table, _ods_table_snapshot} -> load end)

      assert expected == Repo.all(ready_loads_by_table_query)
    end

    test "getting ODS 'ready' loads", %{
      ods_table: ods_table,
      ods_table_snapshot: ods_table_snapshot
    } do
      ods_load_1 =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "ready",
          s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv.gz",
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197,
          is_raw: true
        })

      Repo.insert!(%CubicLoad{
        table_id: ods_table.id,
        status: "ready",
        s3_key: "cubic/ods_qlik/SAMPLE/LOAD2.csv.gz",
        s3_modified: ~U[2022-01-02 20:49:50Z],
        s3_size: 197,
        is_raw: true
      })

      # note: limit to '1' record
      {_table, ready_loads_by_table_query} =
        CubicLoad.get_status_ready_by_table_query({ods_table, ods_table_snapshot}, 1)

      # only get the first load because of limit '1'
      assert [ods_load_1] == Repo.all(ready_loads_by_table_query)

      # add new snapshot load
      new_ods_load =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "ready",
          s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv.gz",
          s3_modified: ~U[2022-01-03 20:49:50Z],
          s3_size: 197,
          is_raw: true
        })

      {_table, new_ready_loads_by_table_query} =
        CubicLoad.get_status_ready_by_table_query({ods_table, ods_table_snapshot})

      # ignoring loads prior to this last snapshot load
      assert [new_ods_load] == Repo.all(new_ready_loads_by_table_query)
    end

    test "no query available because there is no snapshot load", %{
      ods_table: ods_table,
      ods_table_snapshot: ods_table_snapshot
    } do
      # insert a non-snapshot load
      Repo.insert!(%CubicLoad{
        table_id: ods_table.id,
        status: "ready",
        s3_key: "cubic/ods_qlik/SAMPLE/LOAD2.csv.gz",
        s3_modified: ~U[2022-01-02 20:49:50Z],
        s3_size: 197,
        is_raw: true
      })

      assert is_nil(CubicLoad.get_status_ready_by_table_query({ods_table, ods_table_snapshot}))
    end
  end

  describe "ods_load?/1" do
    test "ODS load S3 key is from ODS" do
      assert CubicLoad.ods_load?("cubic/ods_qlik/SAMPLE/LOAD1.csv.gz")
    end

    test "DMAP loads are not from ODS" do
      refute CubicLoad.ods_load?("cubic/dmap/sample/20220101.csv.gz")
    end
  end
end
