defmodule ExCubicOdsIngestion.ProcessIncomingTest do
  use ExCubicOdsIngestion.DataCase

  alias ExCubicOdsIngestion.ProcessIncoming
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  require MockExAws
  require MockExAws.Data

  describe "status/0" do
    test "running state" do
      server = start_supervised!({ProcessIncoming, lib_ex_aws: MockExAws})

      assert ProcessIncoming.status(server) == :running
    end
  end
end
