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

  describe "monitor_athena_query_executions/2" do
    test "returns immediately if all queries have succeeded" do
      query_executions = [
        {:ok, %{"QueryExecutionId" => "success_query_id_1"}},
        {:ok, %{"QueryExecutionId" => "success_query_id_2"}}
      ]

      assert :ok = ExAws.Helpers.monitor_athena_query_executions(MockExAws, query_executions)
    end

    test "returns immediately if any of the queries have failed or were cancelled" do
      query_executions = [
        {:ok, %{"QueryExecutionId" => "cancel_query_id"}},
        {:ok, %{"QueryExecutionId" => "fail_query_id"}}
      ]

      assert {:error, _message} =
               ExAws.Helpers.monitor_athena_query_executions(MockExAws, query_executions)
    end

    test "returns success even if we have a failed query due to an already added partition" do
      query_executions = [
        {:ok, %{"QueryExecutionId" => "success_query_id"}},
        {:ok, %{"QueryExecutionId" => "already_added_partition_query_id"}}
      ]

      assert {:error, _message} =
               ExAws.Helpers.monitor_athena_query_executions(MockExAws, query_executions)
    end
  end
end
