defmodule ExCubicIngestion.Validators do
  @moduledoc """
  Module for holding helpful functions for validation
  """

  @spec valid_iso_date?(String.t()) :: boolean()
  def valid_iso_date?(date_str) do
    match?({:ok, _date}, Date.from_iso8601(date_str))
  end

  @spec valid_iso_datetime?(String.t()) :: boolean()
  def valid_iso_datetime?(datetime_str) do
    match?({:ok, _datetime}, Timex.parse(datetime_str, "{ISO:Extended}"))
  end

  @spec valid_dmap_dataset_url?(String.t()) :: boolean()
  def valid_dmap_dataset_url?(url) do
    parsed_url = URI.parse(url)

    parsed_url.scheme == "https" && parsed_url.path not in [nil, "/"]
  end

  @spec map_has_keys?(map(), [String.t()]) :: boolean()
  def map_has_keys?(map, key_list) do
    Enum.all?(key_list, &Map.has_key?(map, &1))
  end

  @doc """
  S3 object is created in the last 24 hours
  """
  @spec recently_created_s3_object?(map()) :: boolean()
  def recently_created_s3_object?(%{last_modified: last_modified}) do
    {:ok, last_modified_dt, _offset} = DateTime.from_iso8601(last_modified)

    DateTime.compare(last_modified_dt, DateTime.add(DateTime.utc_now(), -86_400)) == :gt
  end

  def recently_created_s3_object?(%{}) do
    false
  end

  @doc """
  Only valid if the name ends with '.csv.gz' and has a size specified
  """
  @spec valid_s3_object?(map()) :: boolean()
  def valid_s3_object?(%{key: key, size: _size}) do
    String.ends_with?(key, ".csv.gz")
  end

  def valid_s3_object?(%{}) do
    false
  end
end
