defmodule ExCubicIngestion.Workers.ArchiveTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  import ExCubicIngestion.TestFixtures, only: [setup_tables_loads: 1]

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot
  alias ExCubicIngestion.Workers.Archive

  require MockExAws

  setup :setup_tables_loads

  describe "perform/1" do
    test "run job for load without metadata file", %{
      dmap_load: dmap_load
    } do
      assert :ok ==
               perform_job(Archive, %{
                 load_rec_id: dmap_load.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "archived" == CubicLoad.get!(dmap_load.id).status
    end

    test "run job for load with metadata file", %{
      ods_load: ods_load
    } do
      assert :ok ==
               perform_job(Archive, %{
                 load_rec_id: ods_load.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "archived" == CubicLoad.get!(ods_load.id).status
    end

    test "run job for load with data file that doesn't exist", %{
      dmap_table: dmap_table
    } do
      dmap_load_unknown =
        Repo.insert!(%CubicLoad{
          table_id: dmap_table.id,
          status: "archiving",
          s3_key: "cubic/dmap/sample/source_does_not_exist.csv.gz",
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197
        })

      assert {:error,
              "Data File: error: \"head_object failed\", Metadata File: ok: \"No metadata file available\""} ==
               perform_job(Archive, %{
                 load_rec_id: dmap_load_unknown.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "archived_unknown" == CubicLoad.get!(dmap_load_unknown.id).status
    end

    test "run job for load with metadata file that doesn't exists", %{
      ods_table: ods_table
    } do
      # only ODS loads have metadata files
      ods_snapshot = ~U[2022-01-01 20:49:50Z]

      ods_load_metadata_unknown =
        Repo.insert!(%CubicLoad{
          table_id: ods_table.id,
          status: "archiving",
          s3_key: "cubic/ods_qlik/SAMPLE/source_metadata_does_not_exist.csv.gz",
          s3_modified: ods_snapshot,
          s3_size: 197
        })

      Repo.insert!(%CubicOdsLoadSnapshot{
        load_id: ods_load_metadata_unknown.id,
        snapshot: ods_snapshot
      })

      assert {:error, "Data File: ok: %{}, Metadata File: error: \"head_object failed\""} ==
               perform_job(Archive, %{
                 load_rec_id: ods_load_metadata_unknown.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "archived_unknown" == CubicLoad.get!(ods_load_metadata_unknown.id).status
    end
  end

  describe "construct_destination_key_root/1" do
    test "getting destination key for generic load", %{
      dmap_load: dmap_load
    } do
      assert "cubic/dmap/sample/20220101" == Archive.construct_destination_key_root(dmap_load)
    end

    test "getting destination key for ODS load", %{
      ods_load: ods_load
    } do
      assert "cubic/ods_qlik/SAMPLE/snapshot=20220101T204950Z/LOAD1" ==
               Archive.construct_destination_key_root(ods_load)
    end
  end
end
