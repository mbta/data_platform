defmodule ExCubicIngestion.Workers.ArchiveTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable
  alias ExCubicIngestion.Workers.Archive

  require MockExAws

  setup do
    # tables
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

    # loads
    dmap_load =
      Repo.insert!(%CubicLoad{
        table_id: dmap_table.id,
        status: "ready_for_archiving",
        s3_key: "cubic/dmap/sample/20220101.csv",
        s3_modified: ~U[2022-01-01 20:49:50Z],
        s3_size: 197
      })

    ods_load =
      Repo.insert!(%CubicLoad{
        table_id: ods_table.id,
        status: "ready_for_archiving",
        s3_key: ods_snapshot_s3_key,
        s3_modified: ods_snapshot,
        s3_size: 197
      })

    # ODS loads
    Repo.insert!(%CubicOdsLoadSnapshot{
      load_id: ods_load.id,
      snapshot: ods_snapshot
    })

    {:ok,
     %{
       dmap_load: dmap_load,
       ods_snapshot: ods_snapshot,
       ods_load: ods_load
     }}
  end

  describe "perform/1" do
    test "run job without error", %{
      dmap_load: dmap_load
    } do
      assert :ok ==
               perform_job(Archive, %{
                 load_rec_id: dmap_load.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "archived" == CubicLoad.get!(dmap_load.id).status
    end
  end

  describe "construct_destination_key/1" do
    test "getting destination key for generic load", %{
      dmap_load: dmap_load
    } do
      assert dmap_load.s3_key == Archive.construct_destination_key(dmap_load)
    end

    test "getting destination key for ODS load", %{
      ods_snapshot: ods_snapshot,
      ods_load: ods_load
    } do
      expected_destination_key =
        Enum.join([
          Path.dirname(ods_load.s3_key),
          '/snapshot=#{Calendar.strftime(ods_snapshot, "%Y%m%dT%H%M%SZ")}/',
          Path.basename(ods_load.s3_key)
        ])

      assert expected_destination_key == Archive.construct_destination_key(ods_load)
    end
  end
end
