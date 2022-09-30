defmodule ExCubicIngestion.StartIngestionTest do
  use ExCubicIngestion.DataCase
  use Oban.Testing, repo: ExCubicIngestion.Repo

  import ExUnit.CaptureLog

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.StartIngestion
  alias ExCubicIngestion.Workers.Ingest

  setup do
    {:ok, state, _timeout} = StartIngestion.init(lib_ex_aws: MockExAws)

    {:ok, state: state}
  end

  describe "status/0" do
    test "running state" do
      server = start_supervised!({StartIngestion, lib_ex_aws: MockExAws})

      assert StartIngestion.status(server) == :running
    end
  end

  describe "run/1" do
    test "schedules ingest job for ready loads", %{state: state} do
      dmap_table =
        Repo.insert!(%CubicTable{
          name: "cubic_dmap__sample",
          s3_prefix: "cubic/dmap/sample/"
        })

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
      dmap_load =
        Repo.insert!(%CubicLoad{
          table_id: dmap_table.id,
          status: "ready",
          s3_key: "cubic/dmap/sample/20220101.csv.gz",
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197
        })

      ods_load =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "ready",
          s3_key: ods_snapshot_s3_key,
          s3_modified: ods_snapshot,
          s3_size: 197
        })

      :ok = StartIngestion.run(state)

      # snapshot was updated
      assert CubicOdsTableSnapshot.get_by!(%{table_id: ods_table.id}).snapshot == ods_snapshot

      # status was updated
      assert CubicLoad.get!(dmap_load.id).status == "ingesting"

      assert CubicLoad.get!(ods_load.id).status == "ingesting"

      # job have been queued
      assert_enqueued(worker: Ingest, args: %{load_rec_ids: [dmap_load.id, ods_load.id]})
    end

    test "ignoring ODS loads without snapshots", %{state: state} do
      dmap_table =
        Repo.insert!(%CubicTable{
          name: "cubic_dmap__sample",
          s3_prefix: "cubic/dmap/sample/"
        })

      ods_table =
        Repo.insert!(%CubicTable{
          name: "cubic_ods_qlik__sample",
          s3_prefix: "cubic/ods_qlik/SAMPLE/"
        })

      # insert ODS table
      Repo.insert!(%CubicOdsTableSnapshot{
        table_id: ods_table.id,
        snapshot: nil,
        snapshot_s3_key: "cubic/ods_qlik/SAMPLE/LOAD1.csv.gz"
      })

      # insert loads
      dmap_load =
        Repo.insert!(%CubicLoad{
          table_id: dmap_table.id,
          status: "ready",
          s3_key: "cubic/dmap/sample/20220101.csv.gz",
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197
        })

      # add a 'ready' CT ODS load
      Repo.insert!(%CubicLoad{
        table_id: ods_table.id,
        status: "ready",
        s3_key: "cubic/ods_qlik/SAMPLE__ct/20220102-204950123.csv.gz",
        s3_modified: ~U[2022-01-02 20:49:50Z],
        s3_size: 197
      })

      :ok = StartIngestion.run(state)

      assert_enqueued(worker: Ingest, args: %{load_rec_ids: [dmap_load.id]})
    end

    test "ready loads with valid and invalid schema", %{state: state} do
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

      invalid_ods_load =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "ready",
          s3_key: "cubic/ods_qlik/SAMPLE/invalid_LOAD2.csv.gz",
          s3_modified: ods_snapshot,
          s3_size: 197
        })

      # capture logs from run
      process_logs =
        capture_log(fn ->
          :ok = StartIngestion.run(state)
        end)

      # status was updated
      assert CubicLoad.get!(ods_load.id).status == "ingesting"

      # valid ones have been queued to ingest
      assert_enqueued(worker: Ingest, args: %{load_rec_ids: [ods_load.id]})

      # for invalid, logged and status was updated to error out
      assert process_logs =~ "[start_ingestion] Invalid schema detected"

      assert CubicLoad.get!(invalid_ods_load.id).status == "ready_for_erroring"
    end
  end

  describe "chunk_loads/3" do
    test "chunking by number of loads" do
      max_num_of_loads = 2
      max_size_of_loads = 1000

      loads = [
        %CubicLoad{
          s3_key: "test/load1.csv.gz",
          s3_size: 12
        },
        %CubicLoad{
          s3_key: "test/load2.csv.gz",
          s3_size: 34
        },
        %CubicLoad{
          s3_key: "test/load3.csv.gz",
          s3_size: 56
        },
        %CubicLoad{
          s3_key: "test/load4.csv.gz",
          s3_size: 78
        },
        %CubicLoad{
          s3_key: "test/load5.csv.gz",
          s3_size: 90
        }
      ]

      expected_chunked_loads = [
        [
          Enum.at(loads, 0),
          Enum.at(loads, 1)
        ],
        [
          Enum.at(loads, 2),
          Enum.at(loads, 3)
        ],
        [
          Enum.at(loads, 4)
        ]
      ]

      assert expected_chunked_loads ==
               StartIngestion.chunk_loads(loads, max_num_of_loads, max_size_of_loads)
    end

    test "chunking by size of loads" do
      max_num_of_loads = 3
      max_size_of_loads = 1000

      loads = [
        %{
          s3_key: "test/load1.csv.gz",
          s3_size: 123
        },
        %{
          s3_key: "test/load2.csv.gz",
          s3_size: 456
        },
        %{
          s3_key: "test/load3.csv.gz",
          s3_size: 789
        },
        %{
          s3_key: "test/load4.csv.gz",
          s3_size: 1000
        },
        %{
          s3_key: "test/load5.csv.gz",
          s3_size: 1112
        }
      ]

      expected_chunked_loads = [
        [
          Enum.at(loads, 0),
          Enum.at(loads, 1)
        ],
        [
          Enum.at(loads, 2)
        ],
        [
          Enum.at(loads, 3)
        ],
        [
          Enum.at(loads, 4)
        ]
      ]

      assert expected_chunked_loads ==
               StartIngestion.chunk_loads(loads, max_num_of_loads, max_size_of_loads)
    end
  end
end
