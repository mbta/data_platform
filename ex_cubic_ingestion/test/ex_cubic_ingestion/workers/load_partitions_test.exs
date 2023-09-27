defmodule ExCubicIngestion.Workers.LoadPartitionsTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Workers.LoadPartitions

  require MockExAws

  describe "perform/1" do
    test "running job" do
      # success
      assert :ok =
               perform_job(LoadPartitions, %{
                 table_names: ["success_table"],
                 lib_ex_aws: "MockExAws"
               })

      # fail
      assert {:error, _message} =
               perform_job(LoadPartitions, %{
                 table_names: ["fail_table"],
                 lib_ex_aws: "MockExAws"
               })
    end
  end

  describe "start_load_partitions/2" do
    test "starting query executions with successes and failures" do
      # both
      assert {[{:ok, _response}], [{:error, {"Exception", _message}}]} =
               LoadPartitions.start_load_partitions(MockExAws, ["success_table", "fail_table"])

      # just successes
      assert {[{:ok, _response}], []} =
               LoadPartitions.start_load_partitions(MockExAws, ["success_table"])

      # just failures
      assert {[], [{:error, {"Exception", _message}}]} =
               LoadPartitions.start_load_partitions(MockExAws, ["fail_table"])
    end
  end
end
