defmodule ExCubicIngestion.Schema.ObanLoggerTest do
  @moduledoc """
  Test for ingest worker error handler.
  """

  use ExCubicIngestion.DataCase

  import ExCubicIngestion.TestFixtures, only: [setup_tables_loads: 1]
  import ExUnit.CaptureLog

  alias ExCubicIngestion.ObanLogger
  alias ExCubicIngestion.Schema.CubicDmapFeed

  setup :setup_tables_loads

  describe "handle_event/4" do
    test "worker start log info" do
      assert capture_log(fn ->
               ObanLogger.handle_event(
                 [:oban, :job, :start],
                 %{},
                 %{queue: "archive", args: []},
                 %{}
               )
             end) =~ "[archive] Start"
    end

    test "worker stop, logs info" do
      assert capture_log(fn ->
               ObanLogger.handle_event(
                 [:oban, :job, :stop],
                 %{duration: 1, queue_time: 1},
                 %{queue: "archive", args: [], state: "", attempt: 1},
                 %{}
               )
             end) =~ "[archive] Stop"
    end

    test "archive worker exception, logs error", %{
      dmap_load: dmap_load
    } do
      assert capture_log(fn ->
               ObanLogger.handle_event(
                 [:oban, :job, :exception],
                 %{duration: 1, queue_time: 1},
                 %{
                   worker: "ExCubicIngestion.Workers.Archive",
                   queue: "archive",
                   args: %{"load_rec_id" => dmap_load.id},
                   state: "",
                   attempt: 1,
                   kind: "",
                   error: "",
                   stacktrace: nil
                 },
                 %{}
               )
             end) =~ dmap_load.s3_key
    end

    test "error worker exception, logs error", %{
      dmap_load: dmap_load
    } do
      assert capture_log(fn ->
               ObanLogger.handle_event(
                 [:oban, :job, :exception],
                 %{duration: 1, queue_time: 1},
                 %{
                   worker: "ExCubicIngestion.Workers.Error",
                   queue: "error",
                   args: %{"load_rec_id" => dmap_load.id},
                   state: "",
                   attempt: 1,
                   kind: "",
                   error: "",
                   stacktrace: nil
                 },
                 %{}
               )
             end) =~ dmap_load.s3_key
    end

    test "fetch dmap worker exception, logs error" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/sample"
        })

      assert capture_log(fn ->
               ObanLogger.handle_event(
                 [:oban, :job, :exception],
                 %{duration: 1, queue_time: 1},
                 %{
                   worker: "ExCubicIngestion.Workers.FetchDmap",
                   queue: "fetch_dmap",
                   args: %{"feed_id" => dmap_feed.id},
                   state: "",
                   attempt: 1,
                   kind: "",
                   error: "",
                   stacktrace: nil
                 },
                 %{}
               )
             end) =~ dmap_feed.relative_url
    end

    test "ingest worker exception, logs error", %{
      dmap_load: dmap_load,
      ods_load: ods_load
    } do
      log =
        capture_log(fn ->
          ObanLogger.handle_event(
            [:oban, :job, :exception],
            %{duration: 1, queue_time: 1},
            %{
              worker: "ExCubicIngestion.Workers.Ingest",
              queue: "ingest",
              args: %{"load_rec_ids" => [ods_load.id, dmap_load.id]},
              state: "",
              attempt: 1,
              kind: "",
              error: "",
              stacktrace: nil
            },
            %{}
          )
        end)

      assert log =~ dmap_load.s3_key
      assert log =~ ods_load.s3_key
    end

    test "catch all worker exception, logs error" do
      assert capture_log(fn ->
               ObanLogger.handle_event(
                 [:oban, :job, :exception],
                 %{duration: 1, queue_time: 1},
                 %{
                   worker: "ExCubicIngestion.Workers.Any",
                   queue: "any",
                   args: %{},
                   state: "",
                   attempt: 1,
                   kind: "",
                   error: "",
                   stacktrace: nil
                 },
                 %{}
               )
             end) =~ "[any] Exception"
    end
  end
end
