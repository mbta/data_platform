defmodule ExCubicIngestion.Workers.FetchDmapTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicDmapDataset
  alias ExCubicIngestion.Schema.CubicDmapFeed
  alias ExCubicIngestion.Workers.FetchDmap

  setup do
    Req.Test.verify_on_exit!()
    Req.Test.stub(MockReq, &MockReq.call/1)
    :ok
  end

  describe "perform/1" do
    test "run job without error" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/datasetpublicusersapi/sample"
        })

      assert :ok ==
               perform_job(FetchDmap, %{
                 feed_id: dmap_feed.id,
                 lib_ex_aws: "MockExAws",
                 lib_req: "MockReq"
               })
    end
  end

  describe "construct_feed_url/3" do
    test "public feed without a last updated timestamp" do
      dmap_feed_relative_url = "/datasetpublicusersapi/sample"

      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: dmap_feed_relative_url
        })

      dmap_base_url = Application.fetch_env!(:ex_cubic_ingestion, :dmap_base_url)

      dmap_public_user_api_key =
        Application.fetch_env!(:ex_cubic_ingestion, :dmap_public_user_api_key)

      assert "#{dmap_base_url}#{dmap_feed_relative_url}?apikey=#{dmap_public_user_api_key}" ==
               FetchDmap.construct_feed_url(dmap_feed, dmap_public_user_api_key, nil)
    end

    test "public feed with a last updated timestamp" do
      dmap_feed_relative_url = "/datasetpublicusersapi/sample"

      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: dmap_feed_relative_url,
          last_updated_at: ~U[2022-05-22 20:49:50.123456Z]
        })

      dmap_base_url = Application.fetch_env!(:ex_cubic_ingestion, :dmap_base_url)

      dmap_public_user_api_key =
        Application.fetch_env!(:ex_cubic_ingestion, :dmap_public_user_api_key)

      assert "#{dmap_base_url}#{dmap_feed_relative_url}?apikey=#{dmap_public_user_api_key}&last_updated=2022-05-22T20:49:50.123457" ==
               FetchDmap.construct_feed_url(dmap_feed, dmap_public_user_api_key, nil)
    end

    test "public feed with last updated passed in" do
      dmap_feed_relative_url = "/datasetpublicusersapi/sample"

      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: dmap_feed_relative_url,
          last_updated_at: ~U[2022-05-22 20:49:50.123456Z]
        })

      last_updated = ~U[2022-05-01 10:49:50.123456Z]

      dmap_base_url = Application.fetch_env!(:ex_cubic_ingestion, :dmap_base_url)

      dmap_public_user_api_key =
        Application.fetch_env!(:ex_cubic_ingestion, :dmap_public_user_api_key)

      assert "#{dmap_base_url}#{dmap_feed_relative_url}?apikey=#{dmap_public_user_api_key}&last_updated=2022-05-01T10:49:50.123456" ==
               FetchDmap.construct_feed_url(
                 dmap_feed,
                 dmap_public_user_api_key,
                 DateTime.to_string(last_updated)
               )
    end
  end

  describe "get_feed_datasets/3" do
    test "sends the API key as a header and query parameter" do
      api_key = Application.fetch_env!(:ex_cubic_ingestion, :dmap_public_user_api_key)
      dmap_feed = %CubicDmapFeed{relative_url: "/datasetpublicusersapi/sample"}

      Req.Test.expect(MockReq, fn conn ->
        assert Plug.Conn.get_req_header(conn, "apikey") == [api_key]
        assert Plug.Conn.fetch_query_params(conn).query_params["apikey"] == api_key
        Req.Test.json(conn, %{"results" => []})
      end)

      assert FetchDmap.get_feed_datasets(dmap_feed, nil, MockReq) == []
    end

    test "does not retry transient HTTP errors" do
      dmap_feed = %CubicDmapFeed{relative_url: "/datasetpublicusersapi/sample"}

      Req.Test.expect(MockReq, fn conn ->
        Plug.Conn.send_resp(conn, 503, "unavailable")
      end)

      assert_raise RuntimeError, ~r/Unable to fetch feed results/, fn ->
        FetchDmap.get_feed_datasets(dmap_feed, nil, MockReq)
      end
    end

    test "unknown feed raises error" do
      dmap_feed_relative_url = "/datasetunknownuserapi/sample"

      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: dmap_feed_relative_url
        })

      assert_raise RuntimeError, fn ->
        FetchDmap.get_feed_datasets(dmap_feed, nil, MockReq)
      end
    end

    test "getting mock controlled feed results" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/datasetcontrolleduserapi/sample",
          last_updated_at: ~U[2022-05-22 20:49:50.123456Z]
        })

      last_updated = ~U[2022-05-01 10:49:50.123456Z]

      assert ["sample_20220517", "sample_20220518"] =
               Enum.map(
                 FetchDmap.get_feed_datasets(
                   dmap_feed,
                   DateTime.to_string(last_updated),
                   MockReq
                 ),
                 & &1["dataset_id"]
               )
    end

    test "getting mock public feed results" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/datasetpublicusersapi/sample",
          last_updated_at: ~U[2022-05-22 20:49:50.123456Z]
        })

      last_updated = ~U[2022-05-01 10:49:50.123456Z]

      assert ["sample_20220517", "sample_20220518"] =
               Enum.map(
                 FetchDmap.get_feed_datasets(
                   dmap_feed,
                   DateTime.to_string(last_updated),
                   MockReq
                 ),
                 & &1["dataset_id"]
               )
    end

    test "getting error response and throwing specific exception with response body" do
      relative_url = "/datasetpublicusersapi/error"

      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: relative_url,
          last_updated_at: ~U[2022-05-22 20:49:50.123456Z]
        })

      last_updated = ~U[2022-05-01 10:49:50.123456Z]

      assert_raise RuntimeError,
                   "Unable to fetch feed results: #{relative_url} (Response: \"{}\")",
                   fn ->
                     FetchDmap.get_feed_datasets(
                       dmap_feed,
                       DateTime.to_string(last_updated),
                       MockReq
                     )
                   end
    end

    test "getting error response and throwing specific exception" do
      relative_url = "/datasetpublicusersapi/exception"

      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: relative_url,
          last_updated_at: ~U[2022-05-22 20:49:50.123456Z]
        })

      last_updated = ~U[2022-05-01 10:49:50.123456Z]

      assert_raise RuntimeError, "Unable to fetch feed results: #{relative_url}", fn ->
        FetchDmap.get_feed_datasets(
          dmap_feed,
          DateTime.to_string(last_updated),
          MockReq
        )
      end
    end
  end

  describe "fetch_and_upload_to_s3/3" do
    test "uploads compressed CSV bytes unchanged" do
      dataset_rec = %CubicDmapDataset{type: "sample", identifier: "sample_20220517"}
      dataset_url = "https://mbtaqadmapdatalake.blob.core.windows.net/sample/sample.csv.gz"
      body = :zlib.gzip("column\nvalue\n")

      Req.Test.expect(MockReq, fn conn ->
        conn
        |> Plug.Conn.put_resp_content_type("application/gzip")
        |> Plug.Conn.put_resp_header("content-encoding", "gzip")
        |> Plug.Conn.send_resp(200, body)
      end)

      assert dataset_rec ==
               FetchDmap.fetch_and_upload_to_s3(
                 {dataset_rec, dataset_url},
                 __MODULE__.CaptureExAws,
                 MockReq
               )

      assert_received {:uploaded, ^body}
    end

    test "rejects HTTP errors instead of uploading them" do
      dataset_rec = %CubicDmapDataset{type: "sample", identifier: "sample_20220517"}

      Req.Test.expect(MockReq, fn conn ->
        Plug.Conn.send_resp(conn, 404, "not found")
      end)

      assert_raise RuntimeError, "Unable to fetch dataset: sample_20220517 (Status: 404)", fn ->
        FetchDmap.fetch_and_upload_to_s3(
          {dataset_rec, "https://example.com/missing.csv.gz"},
          __MODULE__.CaptureExAws,
          MockReq
        )
      end

      refute_received {:uploaded, _}
    end

    test "propagates transport errors without uploading" do
      dataset_rec = %CubicDmapDataset{type: "sample", identifier: "sample_20220517"}

      Req.Test.expect(MockReq, fn conn ->
        Req.Test.transport_error(conn, :timeout)
      end)

      assert_raise Req.TransportError, fn ->
        FetchDmap.fetch_and_upload_to_s3(
          {dataset_rec, "https://example.com/unavailable.csv.gz"},
          __MODULE__.CaptureExAws,
          MockReq
        )
      end

      refute_received {:uploaded, _}
    end

    test "getting file and uploading through mocks" do
      dataset_rec = %CubicDmapDataset{
        type: "sample",
        identifier: "sample_20220517"
      }

      dataset_url =
        "https://mbtaqadmapdatalake.blob.core.windows.net/sample/sample_2022-05-17.csv.gz"

      assert dataset_rec ==
               FetchDmap.fetch_and_upload_to_s3(
                 {dataset_rec, dataset_url},
                 MockExAws,
                 MockReq
               )
    end

    defmodule CaptureExAws do
      @moduledoc false

      @spec request!(ExAws.Operation.t()) :: map()
      def request!(operation) do
        send(self(), {:uploaded, operation.body})
        %{}
      end
    end
  end
end
