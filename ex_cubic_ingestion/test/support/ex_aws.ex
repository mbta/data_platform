defmodule MockExAws do
  @moduledoc """
  Allow for controlling what is returned for a ExAws request.
  """

  @spec request(ExAws.Operation.t(), keyword) :: term
  def request(op, config_overrides \\ [])

  def request(%{service: :s3, http_method: :delete, path: path}, _config_overrides) do
    valid_paths =
      Enum.map(MockExAws.Data.load_objects(), fn load_object ->
        "#{load_object[:key]}"
      end) ++
        [
          "cubic/dmap/sample/already_copied_but_source_not_deleted.csv.gz",
          "cubic/dmap/sample/copy_and_delete.csv.gz",
          "cubic/dmap/sample/20220101.csv"
        ]

    # deleting object
    if Enum.member?(valid_paths, path) do
      {:ok, %{}}
    else
      {:error, %{}}
    end
  end

  def request(%{service: :s3, http_method: :put, headers: headers, path: path}, _config_overrides) do
    incoming_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_incoming)
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    cond do
      # copying object
      headers["x-amz-copy-source"] ==
          "#{incoming_bucket}#{incoming_prefix}does_not_exist/file.csv" ->
        {:error, %{}}

      headers["x-amz-copy-source"] ->
        {:ok, %{}}

      String.starts_with?(path, "#{incoming_prefix}cubic/dmap/sample/") ->
        {:ok, %{}}

      true ->
        {:error, %{}}
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

  def request(%{service: :s3, http_method: :head, path: path}, _config_overrides) do
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    valid_paths = [
      "cubic/dmap/sample/already_copied_but_source_not_deleted.csv.gz",
      "cubic/dmap/sample/timestamp=20220101T000000Z/already_copied_but_source_not_deleted.csv.gz",
      "cubic/dmap/sample/timestamp=20220101T000000Z/already_copied.csv.gz",
      "cubic/dmap/sample/copy_and_delete.csv.gz",
      "#{incoming_prefix}cubic/dmap/sample/20220101.csv"
    ]

    if Enum.member?(valid_paths, path) do
      {:ok, %{}}
    else
      {:error, %{}}
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
