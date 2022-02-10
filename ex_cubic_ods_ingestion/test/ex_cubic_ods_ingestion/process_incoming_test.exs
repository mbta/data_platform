defmodule ExCubicOdsIngestion.ProcessIncomingTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox

  require ExCubicOdsIngestion.Schema.CubicOdsLoad

  # setup server for use throughout tests
  setup do
    # Explicitly get a connection before each test
    :ok = Sandbox.checkout(ExCubicOdsIngestion.Repo)

    # start a supervisor
    server = start_supervised!(ExCubicOdsIngestion.ProcessIncoming)

    %{server: server}
  end

  describe "genserver" do
    test "run", %{server: server} do
      assert ExCubicOdsIngestion.ProcessIncoming.status(server) == :running
    end
  end
end
