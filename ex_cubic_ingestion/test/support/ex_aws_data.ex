defmodule MockExAws.Data do
  @moduledoc """
  Mock data for use in test cases.
  """

  @doc """
  Mock S3 data, in the format returned by `ExAws.S3.load_objects_v2/2`.
  """
  @spec load_objects(String.t()) :: [map()]
  def load_objects(key_starts_with \\ "") do
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    objects = [
      %{
        e_tag: "\"ghi123\"",
        key: "#{incoming_prefix}cubic/dmap/sample/20220101.csv",
        last_modified: "2022-01-01T20:49:50.000Z",
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"jkl123\"",
        key: "#{incoming_prefix}cubic/dmap/sample/20220102.csv",
        last_modified: "2022-01-02T20:49:50.000Z",
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"abc123\"",
        key: "#{incoming_prefix}cubic/ods_qlik/SAMPLE/LOAD1.csv",
        last_modified: "2022-02-08T20:49:50.000Z",
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"def123\"",
        key: "#{incoming_prefix}cubic/ods_qlik/SAMPLE/LOAD2.csv",
        last_modified: "2022-02-08T20:50:50.000Z",
        owner: nil,
        size: "123",
        storage_class: "STANDARD"
      }
    ]

    Enum.filter(objects, &String.starts_with?(&1[:key], key_starts_with))
  end

  @doc """
  Helper function to eliminate duplication in tests that don't care about the bucket prefix.
  """
  @spec load_objects_without_bucket_prefix(String.t()) :: [map()]
  def load_objects_without_bucket_prefix(key_starts_with \\ "") do
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    Enum.map(load_objects(key_starts_with), fn object ->
      %{object | key: String.replace_prefix(object[:key], incoming_prefix, "")}
    end)
  end
end
