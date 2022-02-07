defmodule ExCubicOdsIngestion.ProcessIncomingTest do
  use ExUnit.Case

  # setup server for use throughout tests
  setup do
    server = start_supervised!(ExCubicOdsIngestion.ProcessIncoming)

    %{server: server}
  end

  describe "genserver" do
    test "run", %{server: server} do
      assert ExCubicOdsIngestion.ProcessIncoming.status(server) == :running
    end
  end
end
