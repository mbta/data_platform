defmodule MockExAws.Data do
  @moduledoc """
  Mock data for use in test cases.
  """
  alias ExCubicOdsIngestion.Schema.CubicOdsTable

  @doc """
  A ``CubicOdsTable` which covers the data objects in `load_objects/1`.

  It has not been inserted into the database.
  """
  @spec table :: CubicOdsTable.t()
  def table do
    %CubicOdsTable{
      name: "cubic_ods_qlik__sample",
      s3_prefix: "cubic_ods_qlik/SAMPLE/",
      snapshot_s3_key: "cubic_ods_qlik/SAMPLE/LOAD1.csv"
    }
  end

  @doc """
  Mock S3 data, in the format returned by `ExAws.S3.load_objects_v2/2`.
  """
  @spec load_objects() :: [map()]
  def load_objects do
    [
      %{
        e_tag: "\"abc123\"",
        key: "cubic_ods_qlik/SAMPLE/LOAD1.csv",
        last_modified: "2022-02-08T20:49:50.000Z",
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"def123\"",
        key: "cubic_ods_qlik/SAMPLE/LOAD2.csv",
        last_modified: "2022-02-08T20:50:50.000Z",
        owner: nil,
        size: "123",
        storage_class: "STANDARD"
      }
    ]
  end
end
