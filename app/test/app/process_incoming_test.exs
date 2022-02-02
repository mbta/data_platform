defmodule App.ProcessIncomingTest do
  use ExUnit.Case

  # setup server for use throughout tests
  setup do
    server = start_supervised!(App.ProcessIncoming)

    %{server: server}
  end

  describe "genserver" do

    test "start", %{server: server} do

      # an immediate call to status should indicate that server is 'starting'
      assert App.ProcessIncoming.status(server) == :starting

    end

    test "run", %{server: server} do

      # after waiting @wait_interval_ms + 100, the server should be 'running'
      Process.sleep(1_100)
      assert App.ProcessIncoming.status(server) == :running

    end

  end
end
