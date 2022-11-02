defmodule ExCubicIngestion.ProcessIngestionTest do
  use ExCubicIngestion.DataCase
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.ProcessIngestion
  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Workers.Archive
  alias ExCubicIngestion.Workers.Error
  alias ExCubicIngestion.Workers.Ingest

  require MockExAws.Data
  require Logger

  setup do
    # insert tables
    table =
      Repo.insert!(%CubicTable{
        name: "cubic_dmap__sample",
        s3_prefix: "cubic/dmap/sample/"
      })

    # insert loads
    archive_load =
      Repo.insert!(%CubicLoad{
        table_id: table.id,
        status: "ready_for_archiving",
        s3_key: "cubic/dmap/sample/20220101.csv.gz",
        s3_modified: ~U[2022-01-01 20:49:50Z],
        s3_size: 197
      })

    error_load =
      Repo.insert!(%CubicLoad{
        table_id: table.id,
        status: "ready_for_erroring",
        s3_key: "cubic/dmap/sample/20220102.csv.gz",
        s3_modified: ~U[2022-01-02 20:49:50Z],
        s3_size: 197
      })

    ingest_load =
      Repo.insert!(%CubicLoad{
        table_id: table.id,
        status: "ready_for_ingesting",
        s3_key: "cubic/dmap/sample/20220103.csv.gz",
        s3_modified: ~U[2022-01-02 20:49:50Z],
        s3_size: 197
      })

    {:ok,
     %{
       archive_load: archive_load,
       error_load: error_load,
       ingest_load: ingest_load
     }}
  end

  describe "status/0" do
    test "running state" do
      server = start_supervised!(ProcessIngestion)

      assert ProcessIngestion.status(server) == :running
    end
  end

  describe "process_loads/1" do
    test "processing empty list" do
      assert :ok == ProcessIngestion.process_loads([])
    end

    test "processing with one ready for archiving load and one ready for erroring", %{
      archive_load: archive_load,
      error_load: error_load,
      ingest_load: ingest_load
    } do
      assert :ok == ProcessIngestion.process_loads([archive_load, error_load, ingest_load])
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
               ProcessIngestion.chunk_loads(loads, max_num_of_loads, max_size_of_loads)
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
               ProcessIngestion.chunk_loads(loads, max_num_of_loads, max_size_of_loads)
    end
  end

  describe "archive/1" do
    test "archiving load after ingestion", %{
      archive_load: archive_load
    } do
      # insert job
      ProcessIngestion.archive(archive_load)

      # make sure record is in an "archiving" status
      assert "archiving" == CubicLoad.get!(archive_load.id).status

      assert_enqueued(worker: Archive, args: %{load_rec_id: archive_load.id})
    end
  end

  describe "error/1" do
    test "processing error in ingestion", %{
      error_load: error_load
    } do
      # insert job
      ProcessIngestion.error(error_load)

      # make sure record is in "erroring" status
      assert "erroring" == CubicLoad.get!(error_load.id).status

      assert_enqueued(worker: Error, args: %{load_rec_id: error_load.id})
    end
  end

  describe "ingest/1" do
    test "processing error in ingestion", %{
      ingest_load: ingest_load
    } do
      # insert job
      ProcessIngestion.ingest([ingest_load.id])

      # make sure record is in "ingesting" status
      assert "ingesting" == CubicLoad.get!(ingest_load.id).status

      assert_enqueued(worker: Ingest, args: %{"load_rec_ids" => [ingest_load.id]})
    end
  end
end
