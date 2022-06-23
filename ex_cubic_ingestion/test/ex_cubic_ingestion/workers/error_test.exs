defmodule ExCubicIngestion.Workers.ErrorTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  import ExCubicIngestion.TestFixtures, only: [setup_tables_loads: 1]

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Workers.Error

  require MockExAws

  setup :setup_tables_loads

  describe "perform/1" do
    test "run job without error", %{
      dmap_load: dmap_load
    } do
      assert :ok ==
               perform_job(Error, %{
                 load_rec_id: dmap_load.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "errored" == CubicLoad.get!(dmap_load.id).status
    end

    test "run job for source that doesn't exist", %{
      dmap_table: dmap_table
    } do
      dmap_load_unknown =
        Repo.insert!(%CubicLoad{
          table_id: dmap_table.id,
          status: "erroring",
          s3_key: "cubic/dmap/sample/source_does_not_exist.csv.gz",
          s3_modified: ~U[2022-01-01 20:49:50Z],
          s3_size: 197
        })

      assert {:error, "head_object failed"} ==
               perform_job(Error, %{
                 load_rec_id: dmap_load_unknown.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "errored_unknown" == CubicLoad.get!(dmap_load_unknown.id).status
    end
  end

  describe "construct_destination_key/1" do
    test "getting destination key for generic load", %{
      dmap_load: dmap_load
    } do
      assert dmap_load.s3_key == Error.construct_destination_key(dmap_load)
    end

    test "getting destination key for ODS load", %{
      ods_load: ods_load
    } do
      assert "cubic/ods_qlik/SAMPLE/timestamp=20220101T204950Z/LOAD1.csv" ==
               Error.construct_destination_key(ods_load)
    end
  end
end
