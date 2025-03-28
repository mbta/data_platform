defmodule ExCubicIngestion.ValidateIncomingTest do
  use ExCubicIngestion.DataCase
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.ValidateIncoming

  setup do
    {:ok, state, _timeout} = ValidateIncoming.init(lib_ex_aws: MockExAws)

    {:ok, state: state}
  end

  describe "status/0" do
    test "running state" do
      server = start_supervised!({ValidateIncoming, lib_ex_aws: MockExAws})

      assert ValidateIncoming.status(server) == :running
    end
  end

  describe "run/1" do
    test "ready loads with valid schema", %{state: state} do
      ods_table =
        Repo.insert!(%CubicTable{
          name: "cubic_ods_qlik__sample",
          s3_prefix: "cubic/ods_qlik/SAMPLE/"
        })

      # insert ODS table
      ods_snapshot_s3_key = "cubic/ods_qlik/SAMPLE/LOAD1.csv.gz"
      ods_snapshot = ~U[2022-01-02 20:49:50Z]

      # insert table snapshot with nil value
      Repo.insert!(%CubicOdsTableSnapshot{
        table_id: ods_table.id,
        snapshot: nil,
        snapshot_s3_key: ods_snapshot_s3_key
      })

      # insert loads
      ods_load =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "ready",
          s3_key: ods_snapshot_s3_key,
          s3_modified: ods_snapshot,
          s3_size: 197
        })

      :ok = ValidateIncoming.run(state)

      # status was updated
      assert CubicLoad.get!(ods_load.id).status == "ready_for_ingesting"
    end
  end
end
