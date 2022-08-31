defmodule ExCubicIngestion.TestFixtures do
  @moduledoc """
  Helper functions for setups in tests.
  """

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot
  alias ExCubicIngestion.Schema.CubicOdsTableSnapshot
  alias ExCubicIngestion.Schema.CubicTable

  @spec setup_tables_loads(any()) :: {:ok, map()}
  def setup_tables_loads(_context) do
    # tables
    dmap_table =
      Repo.insert!(%CubicTable{
        name: "cubic_dmap__sample",
        s3_prefix: "cubic/dmap/sample/",
        is_raw: false
      })

    ods_table =
      Repo.insert!(%CubicTable{
        name: "cubic_ods_qlik__sample",
        s3_prefix: "cubic/ods_qlik/SAMPLE/",
        is_raw: true
      })

    # insert ODS table
    ods_snapshot_s3_key = "cubic/ods_qlik/SAMPLE/LOAD1.csv.gz"
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
        status: "ready",
        s3_key: "cubic/dmap/sample/20220101.csv.gz",
        s3_modified: ~U[2022-01-01 20:49:50Z],
        s3_size: 197,
        is_raw: false
      })

    ods_load =
      Repo.insert!(%CubicLoad{
        table_id: ods_table.id,
        status: "ready",
        s3_key: ods_snapshot_s3_key,
        s3_modified: ods_snapshot,
        s3_size: 197,
        is_raw: true
      })

    # ODS loads
    Repo.insert!(%CubicOdsLoadSnapshot{
      load_id: ods_load.id,
      snapshot: ods_snapshot
    })

    {:ok,
     %{
       dmap_table: dmap_table,
       dmap_load: dmap_load,
       ods_table: ods_table,
       ods_load: ods_load
     }}
  end
end
