defmodule MockExAws do
  @moduledoc """
  MockExAws @todo
  """

  @spec request(ExAws.Operation.t(), keyword) :: term
  def request(op, config_overrides \\ [])

  def request(%{service: :s3, http_method: :delete} = op, _config_overrides) do
    incoming_prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_prefix_incoming)

    valid_paths =
      Enum.map(MockExAws.Data.load_objects(), fn load_object ->
        "#{incoming_prefix}#{load_object[:key]}"
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
    incoming_bucket = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_incoming)
    incoming_prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_prefix_incoming)

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
    incoming_prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_prefix_incoming)

    case params do
      %{"prefix" => <<^incoming_prefix, "cubic_ods_qlik_test/">>, "delimiter" => "/"} ->
        {:ok,
         %{
           body: %{
             common_prefixes: [%{prefix: "vendor/SAMPLE/"}],
             contents: [],
             next_continuation_token: ""
           }
         }}

      %{"prefix" => <<^incoming_prefix, "cubic_ods_qlik_test/vendor/SAMPLE/">>} ->
        {:ok,
         %{
           body: %{
             common_prefixes: [],
             contents: MockExAws.Data.load_objects(),
             next_continuation_token: ""
           }
         }}

      %{} ->
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
