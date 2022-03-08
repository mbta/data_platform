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

  describe "filter_to_existing_prefixes/1" do
    test "limits the provided prefixes to those with an existing table" do
      table =
        Repo.insert!(%CubicOdsTable{
          name: "vendor__sample",
          s3_prefix: "vendor/SAMPLE/",
          snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
        })

      prefixes = [
        "vendor/SAMPLE/",
        "vendor/SAMPLE__ct/",
        "vendor/SAMPLE_TABLE_WRONG/",
        "other"
      ]

      expected = [
        {"vendor/SAMPLE/", table},
        {"vendor/SAMPLE__ct/", table}
      ]

      actual = CubicOdsTable.filter_to_existing_prefixes(prefixes)

      assert expected == actual
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
