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

    utc_now = DateTime.utc_now()

    objects = [
      %{
        e_tag: "\"ghi123\"",
        key: "#{incoming_prefix}cubic/dmap/sample/20220101.csv.gz",
        last_modified: dt_adjust_and_format(utc_now, -3600),
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"jkl123\"",
        key: "#{incoming_prefix}cubic/dmap/sample/20220102.csv.gz",
        last_modified: dt_adjust_and_format(utc_now, -3000),
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"abc123\"",
        key: "#{incoming_prefix}cubic/ods_qlik/SAMPLE/LOAD1.csv.gz",
        last_modified: dt_adjust_and_format(utc_now, -2400),
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"abc123\"",
        key: "#{incoming_prefix}cubic/ods_qlik/SAMPLE/LOAD1.dfm",
        last_modified: dt_adjust_and_format(utc_now, -2400),
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"def123\"",
        key: "#{incoming_prefix}cubic/ods_qlik/SAMPLE/LOAD2.csv.gz",
        last_modified: dt_adjust_and_format(utc_now, -1800),
        owner: nil,
        size: "123",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"def123\"",
        key: "#{incoming_prefix}cubic/ods_qlik/SAMPLE/LOAD2.dfm",
        last_modified: dt_adjust_and_format(utc_now, -1800),
        owner: nil,
        size: "123",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"abc123\"",
        key: "#{incoming_prefix}cubic/ods_qlik/SAMPLE__ct/20211201-112233444.csv.gz",
        last_modified: dt_adjust_and_format(utc_now, -1200),
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"abc123\"",
        key: "#{incoming_prefix}cubic/ods_qlik/SAMPLE__ct/20211201-112233444.dfm",
        last_modified: dt_adjust_and_format(utc_now, -1200),
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"def123\"",
        key: "#{incoming_prefix}cubic/ods_qlik/SAMPLE__ct/20211201-122433444.csv.gz",
        last_modified: dt_adjust_and_format(utc_now, -600),
        owner: nil,
        size: "123",
        storage_class: "STANDARD"
      },
      %{
        e_tag: "\"def123\"",
        key: "#{incoming_prefix}cubic/ods_qlik/SAMPLE__ct/20211201-122433444.dfm",
        last_modified: dt_adjust_and_format(utc_now, -600),
        owner: nil,
        size: "123",
        storage_class: "STANDARD"
      }
    ]

    Enum.filter(objects, &String.starts_with?(&1.key, key_starts_with))
  end

  @doc """
  For a give datetime, adjust it by with the seconds and format it as an S3 timestamp.
  """
  @spec dt_adjust_and_format(DateTime.t(), Integer.t()) :: String.t()
  def dt_adjust_and_format(dt, seconds) do
    dt |> DateTime.add(seconds, :second) |> Calendar.strftime("%Y-%m-%dT%H:%M:%S.000Z")
  end
end
