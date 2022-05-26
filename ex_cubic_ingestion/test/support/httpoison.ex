defmodule MockHTTPoison do
  @moduledoc """
  Allow for controlling what is returned for a HTTPoison request.
  """

  @spec get!(String.t()) :: HTTPoison.Response.t()
  def get!(_url) do
    %HTTPoison.Response{status_code: 200, body: "
      {
        \"success\": true,
        \"results\": [
          {
            \"id\": \"sample\",
            \"dataset_id\": \"sample_20220517\",
            \"url\": \"https://mbtaqadmapdatalake.blob.core.windows.net/sample/sample_2022-05-17.csv.gz\",
            \"start_date\": \"2022-05-17\",
            \"end_date\": \"2022-05-17\",
            \"last_updated\": \"2022-05-18T13:39:43.546303\"
          },
          {
            \"id\": \"sample\",
            \"dataset_id\": \"sample_20220518\",
            \"url\": \"https://mbtaqadmapdatalake.blob.core.windows.net/sample/sample_2022-05-18.csv.gz\",
            \"start_date\": \"2022-05-18\",
            \"end_date\": \"2022-05-18\",
            \"last_updated\": \"2022-05-19T12:12:44.737440\"
          }
        ]
      }"}
  end
end
