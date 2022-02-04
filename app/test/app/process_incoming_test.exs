defmodule App.ProcessIncomingTest do
  use ExUnit.Case

  # setup server for use throughout tests
  setup do
    server = start_supervised!(App.ProcessIncoming)

    %{server: server}
  end

  describe "genserver" do

    test "run", %{server: server} do

      assert App.ProcessIncoming.status(server) == :running

    end

  end
end
