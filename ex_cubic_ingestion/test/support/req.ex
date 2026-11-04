defmodule MockReq do
  @moduledoc """
  Run Req requests against a test-owned HTTP stub.
  """

  @spec get(String.t(), keyword()) :: {:ok, Req.Response.t()} | {:error, Exception.t()}
  def get(url, options \\ []) do
    Req.get(url, Keyword.put(options, :plug, {Req.Test, __MODULE__}))
  end

  @spec get!(String.t(), keyword()) :: Req.Response.t()
  def get!(url, options \\ []) do
    Req.get!(url, Keyword.put(options, :plug, {Req.Test, __MODULE__}))
  end

  @spec call(Plug.Conn.t()) :: Plug.Conn.t()
  def call(conn) do
    cond do
      String.starts_with?(conn.request_path, "/datasetpublicusersapi/sample") or
          String.starts_with?(conn.request_path, "/datasetcontrolleduserapi/sample") ->
        Req.Test.json(conn, %{
          "success" => true,
          "results" => [
            %{
              "id" => "sample",
              "dataset_id" => "sample_20220517",
              "url" =>
                "https://mbtaqadmapdatalake.blob.core.windows.net/sample/sample_2022-05-17.csv.gz",
              "start_date" => "2022-05-17",
              "end_date" => "2022-05-17",
              "last_updated" => "2022-05-18T13:39:43.546303"
            },
            %{
              "id" => "sample",
              "dataset_id" => "sample_20220518",
              "url" =>
                "https://mbtaqadmapdatalake.blob.core.windows.net/sample/sample_2022-05-18.csv.gz",
              "start_date" => "2022-05-18",
              "end_date" => "2022-05-18",
              "last_updated" => "2022-05-19T12:12:44.737440"
            }
          ]
        })

      String.starts_with?(conn.request_path, "/datasetpublicusersapi/error") ->
        # bad request
        Plug.Conn.send_resp(conn, 400, "{}")

      conn.host == "mbtaqadmapdatalake.blob.core.windows.net" and
          String.starts_with?(conn.request_path, "/sample") ->
        Plug.Conn.send_resp(conn, 200, "sample_body")

      true ->
        Req.Test.transport_error(conn, :timeout)
    end
  end
end
