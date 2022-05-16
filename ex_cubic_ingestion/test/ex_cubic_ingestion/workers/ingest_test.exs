defmodule ExCubicIngestion.Workers.IngestTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Workers.Ingest

  require MockExAws

  setup do
    # insert tables
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
    ods_snapshot_s3_key = "cubic/ods_qlik/SAMPLE/LOAD1.csv"
    ods_snapshot = ~U[2022-01-01 20:49:50Z]

    Repo.insert!(%CubicOdsTableSnapshot{
      table_id: ods_table.id,
      snapshot: ods_snapshot,
      snapshot_s3_key: ods_snapshot_s3_key
    })

    # insert loads
    dmap_load_1 =
      Repo.insert!(%CubicLoad{
        table_id: dmap_table.id,
        status: "ready",
        s3_key: "cubic/dmap/sample/20220101.csv",
        s3_modified: ~U[2022-01-01 20:49:50Z],
        s3_size: 197
      })

    dmap_load_2 =
      Repo.insert!(%CubicLoad{
        table_id: dmap_table.id,
        status: "ready",
        s3_key: "cubic/dmap/sample/20220102.csv",
        s3_modified: ~U[2022-01-02 20:49:50Z],
        s3_size: 197
      })

    ods_load_1 =
      Repo.insert!(%CubicLoad{
        table_id: ods_table.id,
        status: "ready",
        s3_key: ods_snapshot_s3_key,
        s3_modified: ods_snapshot,
        s3_size: 197
      })

    ods_load_2 =
      Repo.insert!(%CubicLoad{
        table_id: ods_table.id,
        status: "ready",
        s3_key: "cubic/ods_qlik/SAMPLE/LOAD2.csv",
        s3_modified: ~U[2022-01-01 20:50:50Z],
        s3_size: 197
      })

    # insert ODS loads
    Repo.insert!(%CubicOdsLoadSnapshot{
      load_id: ods_load_1.id,
      snapshot: ods_snapshot
    })

    Repo.insert!(%CubicOdsLoadSnapshot{
      load_id: ods_load_2.id,
      snapshot: ods_snapshot
    })

    {:ok,
     %{
       dmap_table: dmap_table,
       dmap_load_1: dmap_load_1,
       dmap_load_2: dmap_load_2,
       ods_snapshot: ods_snapshot,
       ods_table: ods_table,
       ods_load_1: ods_load_1,
       ods_load_2: ods_load_2
     }}
  end

  describe "perform/1" do
    test "run job", %{
      dmap_load_1: dmap_load_1,
      dmap_load_2: dmap_load_2,
      ods_load_1: ods_load_1,
      ods_load_2: ods_load_2
    } do
      assert :ok =
               perform_job(Ingest, %{
                 load_rec_ids: [dmap_load_1.id, dmap_load_2.id, ods_load_1.id, ods_load_2.id],
                 lib_ex_aws: "MockExAws"
               })

      assert "ready_for_archiving" == CubicLoad.get!(dmap_load_1.id).status
    end
  end

  describe "construct_glue_job_payload/1" do
    test "payload is contructed correctly with ods and dmap data", %{
      dmap_table: dmap_table,
      dmap_load_1: dmap_load_1,
      dmap_load_2: dmap_load_2,
      ods_snapshot: ods_snapshot,
      ods_table: ods_table,
      ods_load_1: ods_load_1,
      ods_load_2: ods_load_2
    } do
      {_actual_env, actual_input_with_generic_ods} =
        Ingest.construct_glue_job_payload([
          dmap_load_1.id,
          dmap_load_2.id,
          ods_load_1.id,
          ods_load_2.id
        ])

      actual_input_with_generic_ods_decoded = Jason.decode!(actual_input_with_generic_ods)

      expected_input_with_generic_ods = %{
        "generic_loads" => [
          %{
            "id" => dmap_load_1.id,
            "s3_key" => dmap_load_1.s3_key,
            "table_name" => dmap_table.name
          },
          %{
            "id" => dmap_load_2.id,
            "s3_key" => dmap_load_2.s3_key,
            "table_name" => dmap_table.name
          }
        ],
        "ods_loads" => [
          %{
            "id" => ods_load_1.id,
            "s3_key" => ods_load_1.s3_key,
            "table_name" => ods_table.name,
            "snapshot" => Calendar.strftime(ods_snapshot, "%Y%m%dT%H%M%SZ")
          },
          %{
            "id" => ods_load_2.id,
            "s3_key" => ods_load_2.s3_key,
            "table_name" => ods_table.name,
            "snapshot" => Calendar.strftime(ods_snapshot, "%Y%m%dT%H%M%SZ")
          }
        ]
      }

      assert expected_input_with_generic_ods["generic_loads"] ==
               Enum.sort(
                 actual_input_with_generic_ods_decoded["generic_loads"],
                 &(&1["id"] < &2["id"])
               )

      assert expected_input_with_generic_ods["ods_loads"] ==
               Enum.sort(
                 actual_input_with_generic_ods_decoded["ods_loads"],
                 &(&1["id"] < &2["id"])
               )

      {_actual_env, actual_input_with_generic} =
        Ingest.construct_glue_job_payload([dmap_load_1.id, dmap_load_2.id])

      actual_input_with_generic_decoded = Jason.decode!(actual_input_with_generic)

      expected_input_with_generic = %{
        "generic_loads" => [
          %{
            "id" => dmap_load_1.id,
            "s3_key" => dmap_load_1.s3_key,
            "table_name" => dmap_table.name
          },
          %{
            "id" => dmap_load_2.id,
            "s3_key" => dmap_load_2.s3_key,
            "table_name" => dmap_table.name
          }
        ],
        "ods_loads" => []
      }

      assert expected_input_with_generic["generic_loads"] ==
               Enum.sort(
                 actual_input_with_generic_decoded["generic_loads"],
                 &(&1["id"] < &2["id"])
               )

      assert expected_input_with_generic["ods_loads"] ==
               actual_input_with_generic_decoded["ods_loads"]

      {_actual_env, actual_input_with_ods} =
        Ingest.construct_glue_job_payload([ods_load_1.id, ods_load_2.id])

      actual_input_with_ods_decoded = Jason.decode!(actual_input_with_ods)

      expected_input_with_ods = %{
        "generic_loads" => [],
        "ods_loads" => [
          %{
            "id" => ods_load_1.id,
            "s3_key" => ods_load_1.s3_key,
            "table_name" => ods_table.name,
            "snapshot" => Calendar.strftime(ods_snapshot, "%Y%m%dT%H%M%SZ")
          },
          %{
            "id" => ods_load_2.id,
            "s3_key" => ods_load_2.s3_key,
            "table_name" => ods_table.name,
            "snapshot" => Calendar.strftime(ods_snapshot, "%Y%m%dT%H%M%SZ")
          }
        ]
      }

      assert expected_input_with_ods["generic_loads"] ==
               actual_input_with_ods_decoded["generic_loads"]

      assert expected_input_with_ods["ods_loads"] ==
               Enum.sort(actual_input_with_ods_decoded["ods_loads"], &(&1["id"] < &2["id"]))
    end
  end
end