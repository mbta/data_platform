defmodule ExCubicIngestion.Validators do
  @moduledoc """
  Module for holding helpful functions for validation
  """

  @spec is_valid_iso_date?(String.t()) :: boolean()
  def is_valid_iso_date?(date_str) do
    match?({:ok, _date}, Date.from_iso8601(date_str))
  end

  @spec is_valid_iso_datetime?(String.t()) :: boolean()
  def is_valid_iso_datetime?(datetime_str) do
    match?({:ok, _datetime}, Timex.parse(datetime_str, "{ISO:Extended}"))
  end

  @spec is_valid_dmap_dataset_url?(String.t()) :: boolean()
  def is_valid_dmap_dataset_url?(url) do
    URI.parse(url).scheme == "https" &&
      URI.parse(url).path not in [nil, "/"]
  end

  @spec map_has_keys?(map(), [String.t()]) :: boolean()
  def map_has_keys?(map, key_list) do
    Enum.all?(key_list, &Map.has_key?(map, &1))
  end
end
