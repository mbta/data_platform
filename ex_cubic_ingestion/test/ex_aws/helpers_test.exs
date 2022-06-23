defmodule ExAws.HelpersTest do
  use ExCubicIngestion.DataCase, async: true

  describe "move/5" do
    test "already copied but there is still a source object" do
      assert {:ok, _} =
               ExAws.Helpers.move(
                 MockExAws,
                 # incoming bucket
                 "incoming",
                 "cubic/dmap/sample/already_copied_but_source_not_deleted.csv.gz",
                 # error bucket
                 "error",
                 "cubic/dmap/sample/timestamp=20220101T000000Z/already_copied_but_not_deleted.csv.gz"
               )
    end

    test "already copied" do
      assert {:ok, _} =
               ExAws.Helpers.move(
                 MockExAws,
                 # incoming bucket
                 "incoming",
                 "cubic/dmap/sample/already_copied.csv.gz",
                 # error bucket
                 "error",
                 "cubic/dmap/sample/timestamp=20220101T000000Z/already_copied.csv.gz"
               )
    end

    test "source object does not exist" do
      assert {:error, "head_object failed"} =
               ExAws.Helpers.move(
                 MockExAws,
                 # incoming bucket
                 "incoming",
                 "cubic/dmap/sample/source_does_not_exist.csv.gz",
                 # error bucket
                 "error",
                 "cubic/dmap/sample/timestamp=20220101T000000Z/source_does_not_exist.csv.gz"
               )
    end

    test "do the move action" do
      assert {:ok, _} =
               ExAws.Helpers.move(
                 MockExAws,
                 # incoming bucket
                 "incoming",
                 "cubic/dmap/sample/move.csv.gz",
                 # error bucket
                 "error",
                 "cubic/dmap/sample/timestamp=20220101T000000Z/move.csv.gz"
               )
    end

    test "try to move with copy failing" do
      assert {:error, "copy_object failed"} =
               ExAws.Helpers.move(
                 MockExAws,
                 # incoming bucket
                 "incoming",
                 "cubic/dmap/sample/move_with_copy_failing.csv.gz",
                 # error bucket
                 "error",
                 "cubic/dmap/sample/timestamp=20220101T000000Z/move_with_copy_failing.csv.gz"
               )
    end
  end
end
