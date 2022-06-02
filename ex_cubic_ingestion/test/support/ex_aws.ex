defmodule MockExAws do
  @moduledoc """
  Allow for controlling what is returned for a ExAws request.
  """

  @spec request(ExAws.Operation.t(), keyword) :: term
  def request(op, config_overrides \\ [])

  def request(%{service: :s3, http_method: :delete} = op, _config_overrides) do
    valid_paths =
      Enum.map(MockExAws.Data.load_objects(), fn load_object ->
        "#{load_object[:key]}"
      end)

    # deleting object
    if Enum.member?(valid_paths, op.path) do
      {:ok,
       [
         body: "",
         headers: [
           {"x-amz-id-2", "abc123"},
           {"x-amz-request-id", "abc123"},
           {"Date", "Wed, 02 Mar 2022 20:11:48 GMT"},
           {"Server", "AmazonS3"}
         ],
         status_code: 204
       ]}
    else
      {:error,
       [
         body: "",
         headers: [
           {"x-amz-id-2", "abc123"},
           {"x-amz-request-id", "abc123"},
           {"Date", "Wed, 02 Mar 2022 20:11:48 GMT"},
           {"Server", "AmazonS3"}
         ],
         status_code: 404
       ]}
    end
  end

  def request(%{service: :s3, http_method: :put} = op, _config_overrides) do
    incoming_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_incoming)
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    cond do
      # copying object
      op.headers["x-amz-copy-source"] ==
          "#{incoming_bucket}#{incoming_prefix}does_not_exist/file.csv" ->
        {:error,
         [
           body:
             "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LastModified>2022-03-02T20:11:47.000Z</LastModified><ETag>&quot;abc123&quot;</ETag></CopyObjectResult>",
           headers: [
             {"x-amz-id-2", "abc123"},
             {"x-amz-request-id", "abc123"},
             {"Date", "Wed, 02 Mar 2022 20:11:47 GMT"},
             {"x-amz-server-side-encryption", "aws:kms"},
             {"x-amz-server-side-encryption-aws-kms-key-id", ""},
             {"Content-Type", "application/xml"},
             {"Server", "AmazonS3"},
             {"Content-Length", "234"}
           ],
           status_code: 404
         ]}

      op.headers["x-amz-copy-source"] ->
        {:ok,
         [
           body:
             "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LastModified>2022-03-02T20:11:47.000Z</LastModified><ETag>&quot;abc123&quot;</ETag></CopyObjectResult>",
           headers: [
             {"x-amz-id-2", "abc123"},
             {"x-amz-request-id", "abc123"},
             {"Date", "Wed, 02 Mar 2022 20:11:47 GMT"},
             {"x-amz-server-side-encryption", "aws:kms"},
             {"x-amz-server-side-encryption-aws-kms-key-id", ""},
             {"Content-Type", "application/xml"},
             {"Server", "AmazonS3"},
             {"Content-Length", "234"}
           ],
           status_code: 200
         ]}

      true ->
        {:error,
         [
           body: "",
           headers: [],
           status_code: 404
         ]}
    end
  end

  def request(
        %{
          service: :s3,
          http_method: :get,
          params: params
        },
        _config_overrides
      ) do
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    cubic = incoming_prefix <> "cubic/"
    cubic_ods_qlik = cubic <> "ods_qlik/"
    cubic_ods_qlik_sample = cubic_ods_qlik <> "SAMPLE/"
    cubic_dmap = cubic <> "dmap/"
    cubic_dmap_sample = cubic_dmap <> "sample/"

    case params do
      %{"prefix" => ^cubic_ods_qlik, "delimiter" => "/"} ->
        {:ok,
         %{
           body: %{
             common_prefixes: [%{prefix: cubic_ods_qlik_sample}],
             contents: [],
             next_continuation_token: ""
           }
         }}

      %{"prefix" => ^cubic_dmap, "delimiter" => "/"} ->
        {:ok,
         %{
           body: %{
             common_prefixes: [%{prefix: cubic_dmap_sample}],
             contents: [],
             next_continuation_token: ""
           }
         }}

      %{"prefix" => ^cubic_dmap_sample} ->
        {:ok,
         %{
           body: %{
             common_prefixes: [],
             contents: MockExAws.Data.load_objects(cubic_dmap_sample),
             next_continuation_token: ""
           }
         }}

      %{"prefix" => ^cubic_ods_qlik_sample} ->
        {:ok,
         %{
           body: %{
             common_prefixes: [],
             contents: MockExAws.Data.load_objects(cubic_ods_qlik_sample),
             next_continuation_token: ""
           }
         }}

      false ->
        {:ok,
         %{
           body: %{
             common_prefixes: [],
             contents: [],
             next_continuation_token: ""
           }
         }}
    end
  end

  def request(%{service: :glue, data: %{RunId: "success_run_id"}}, _config_overrides) do
    {:ok, %{"JobRun" => %{"JobRunState" => "SUCCEEDED"}}}
  end

  def request(%{service: :glue, data: %{RunId: "error_run_id"}}, _config_overrides) do
    {:ok, %{"JobRun" => %{"JobRunState" => "ERROR"}}}
  end

  def request(%{service: :glue} = op, _config_overrides) do
    cond do
      Enum.member?(op.headers, {"x-amz-target", "AWSGlue.StartJobRun"}) ->
        {:ok, %{"JobRunId" => "abc123"}}

      Enum.member?(op.headers, {"x-amz-target", "AWSGlue.GetJobRun"}) ->
        {:ok, %{"JobRun" => %{"JobRunState" => "SUCCEEDED"}}}

      true ->
        {:error, %{}}
    end
  end

  def request(%ExAws.S3.Upload{service: :s3} = op, _config_overrides) do
    {:ok,
     %{
       body:
         "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Location>https://s3.amazonaws.com/mbta-ctd-dataplatform-local/ggjura%2Fincoming%2Fdmap%2Fsample%2Fsample_20220517.csv.gz</Location><Bucket>mbta-ctd-dataplatform-local</Bucket><Key>ggjura/incoming/dmap/sample/sample_20220517.csv.gz</Key><ETag>&quot;1a0196d50ce8c2e45cd82a6999ee257b-1&quot;</ETag></CompleteMultipartUploadResult>",
       headers: [
         {"x-amz-id-2",
          "T3ev12loW6XQmNxUnkhN7hspi409yvtPbe8tmx8higrhITw04AHLFKjp9/J5GpZdnmr44AqcbZs="},
         {"x-amz-request-id", "SV1Y5G934WRCAFQJ"},
         {"Date", "Thu, 02 Jun 2022 15:06:11 GMT"},
         {"x-amz-server-side-encryption", "aws:kms"},
         {"x-amz-server-side-encryption-aws-kms-key-id",
          "arn:aws:kms:us-east-1:434035161053:key/df815a77-d5a8-4b1a-ba48-373633a52f44"},
         {"Content-Type", "application/xml"},
         {"Transfer-Encoding", "chunked"},
         {"Server", "AmazonS3"}
       ],
       status_code: 200
     }}
  end

  def request(%{service: :s3, http_method: :head, path: path} = op, _config_overrides) do
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    cubic = incoming_prefix <> "cubic/"
    cubic_dmap = cubic <> "dmap/"
    cubic_dmap_sample = cubic_dmap <> "sample/"
    cubic_dmap_sample_path = "#{incoming_prefix}#{cubic_dmap_sample}sample_20220517.csv.gz"

    if path == cubic_dmap_sample_path do
      {:ok,
       %{
         headers: [
           {"x-amz-id-2",
            "LlGseP4G6aShfC2gsw6eGToDE1euJawXyhgMHPWcymczFB/GoKnpD2obCdOjAxo+7PciGuOnIJQ="},
           {"x-amz-request-id", "SV1H4HGXNVESFZDZ"},
           {"Date", "Thu, 02 Jun 2022 15:06:11 GMT"},
           {"Last-Modified", "Thu, 02 Jun 2022 15:06:10 GMT"},
           {"ETag", "\"1a0196d50ce8c2e45cd82a6999ee257b-1\""},
           {"x-amz-server-side-encryption", "aws:kms"},
           {"x-amz-server-side-encryption-aws-kms-key-id",
            "arn:aws:kms:us-east-1:434035161053:key/df815a77-d5a8-4b1a-ba48-373633a52f44"},
           {"Accept-Ranges", "bytes"},
           {"Content-Type", "application/octet-stream"},
           {"Server", "AmazonS3"},
           {"Content-Length", "40322"}
         ],
         status_code: 200
       }}
    else
      {:ok,
       %{
         headers: [],
         status_code: nil
       }}
    end
  end

  @spec request!(ExAws.Operation.t(), keyword) :: term
  def request!(op, config_overrides \\ []) do
    case request(op, config_overrides) do
      {:ok, result} ->
        result

      error ->
        raise ExAws.Error, """
        ExAws Request Error!
        #{inspect(error)}
        """
    end
  end
end
