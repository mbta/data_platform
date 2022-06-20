defmodule ExAws.HelpersTest do
  use ExCubicIngestion.DataCase, async: true

  describe "copy_and_delete/5" do
    test "already copied but there is still a source object" do
      assert :ok =
               ExAws.Helpers.copy_and_delete(
                 MockExAws,
                 # incoming bucket
                 "",
                 "cubic/dmap/sample/already_copied_but_source_not_deleted.csv.gz",
                 # error bucket
                 "",
                 "cubic/dmap/sample/timestamp=20220101T000000Z/already_copied_but_not_deleted.csv.gz"
               )
    end

    test "already copied" do
      assert :ok =
               ExAws.Helpers.copy_and_delete(
                 MockExAws,
                 # incoming bucket
                 "",
                 "cubic/dmap/sample/already_copied.csv.gz",
                 # error bucket
                 "",
                 "cubic/dmap/sample/timestamp=20220101T000000Z/already_copied.csv.gz"
               )
    end

    test "source object does not exist" do
      assert :error =
               ExAws.Helpers.copy_and_delete(
                 MockExAws,
                 # incoming bucket
                 "",
                 "cubic/dmap/sample/source_does_not_exist.csv.gz",
                 # error bucket
                 "",
                 "cubic/dmap/sample/timestamp=20220101T000000Z/source_does_not_exist.csv.gz"
               )
    end

    test "do the copy and delete action" do
      assert :ok =
               ExAws.Helpers.copy_and_delete(
                 MockExAws,
                 # incoming bucket
                 "",
                 "cubic/dmap/sample/copy_and_delete.csv.gz",
                 # error bucket
                 "",
                 "cubic/dmap/sample/timestamp=20220101T000000Z/copy_and_delete.csv.gz"
               )
    end
  end
end
