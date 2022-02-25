defmodule ExCubicOdsIngestion.Schema.CubicOdsTableTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsTable

  setup do
    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)
  end

  describe "get/1" do
    test "adding and getting table rec" do
      # add table
      new_table_rec = %CubicOdsTable{
        name: "vendor__sample",
        s3_prefix: "vendor/SAMPLE/",
        snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
      }

      {:ok, inserted_table_rec} =
        Repo.transaction(fn ->
          Repo.insert!(new_table_rec)
        end)

      assert inserted_table_rec == CubicOdsTable.get!(inserted_table_rec.id)
    end
  end

  describe "get_from_load_s3_key/1" do
    test "getting inserted record" do
      new_table_rec = %CubicOdsTable{
        name: "vendor__sample",
        s3_prefix: "vendor/SAMPLE/",
        snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
      }

      {:ok, inserted_table_rec} =
        Repo.transaction(fn ->
          Repo.insert!(new_table_rec)
        end)

      assert inserted_table_rec == CubicOdsTable.get_from_load_s3_key("vendor/SAMPLE/LOAD1.csv")
    end

    test "getting nothing back with non-existing key" do
      assert nil == CubicOdsTable.get_from_load_s3_key("not/in/db.csv")
    end
  end

  describe "update/2" do
    test "updating the snapshot" do
      # add table
      new_table_rec = %CubicOdsTable{
        name: "vendor__sample",
        s3_prefix: "vendor/SAMPLE/",
        snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
      }

      {:ok, inserted_table_rec} =
        Repo.transaction(fn ->
          Repo.insert!(new_table_rec)
        end)

      dt = DateTime.now!("Etc/UTC")
      dt_without_msec = DateTime.truncate(dt, :second)
      updated_table_rec = CubicOdsTable.update(inserted_table_rec, snapshot: dt_without_msec)

      assert dt_without_msec == updated_table_rec.snapshot
    end
  end
end
