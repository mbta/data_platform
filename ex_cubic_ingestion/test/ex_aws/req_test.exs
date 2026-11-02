defmodule ExAws.ReqTest do
  use ExUnit.Case, async: true

  alias ExAws.Request.Req, as: ReqClient

  setup do
    Req.Test.verify_on_exit!()
  end

  test "AWS requests use Req and preserve response bodies for AWS parsers" do
    assert Application.fetch_env!(:ex_aws, :http_client) == ReqClient

    Req.Test.expect(__MODULE__, fn conn ->
      assert conn.method == "GET"
      assert Plug.Conn.get_req_header(conn, "authorization") == ["signed-request"]

      conn
      |> Plug.Conn.put_resp_content_type("application/xml")
      |> Plug.Conn.send_resp(200, "<ListBucketResult/>")
    end)

    assert {:ok, %{status_code: 200, body: "<ListBucketResult/>"}} =
             ReqClient.request(
               :get,
               "https://s3.example.com/bucket",
               "",
               [{"authorization", "signed-request"}],
               plug: {Req.Test, __MODULE__}
             )
  end
end
