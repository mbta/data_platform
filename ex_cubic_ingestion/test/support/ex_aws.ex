defmodule MockExAws do
  @moduledoc """
  Allow for controlling what is returned for a ExAws request.
  """

  @spec request(ExAws.Operation.t(), keyword) :: term
  def request(op, config_overrides \\ [])

  def request(%{service: :s3, http_method: :delete, path: path}, _config_overrides) do
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    valid_paths =
      Enum.map(MockExAws.Data.load_objects(), fn load_object ->
        "#{load_object[:key]}"
      end) ++
        [
          "cubic/dmap/sample/already_copied_but_source_not_deleted.csv.gz",
          "cubic/dmap/sample/move.csv.gz",
          "#{incoming_prefix}cubic/ods_qlik/SAMPLE/source_metadata_does_not_exist.csv.gz"
        ]

    # deleting object
    if Enum.member?(valid_paths, path) do
      {:ok, %{}}
    else
      {:error, "delete_object failed"}
    end
  end

  def request(%{service: :s3, http_method: :put, headers: headers, path: path}, _config_overrides) do
    incoming_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_incoming)
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    # only fail copying for these paths
    error_copy_paths = [
      "#{incoming_bucket}#{incoming_prefix}does_not_exist/file.csv.gz",
      "/incoming/cubic/dmap/sample/move_with_copy_failing.csv.gz"
    ]

    cond do
      # copying object
      Enum.member?(error_copy_paths, headers["x-amz-copy-source"]) ->
        {:error, "copy_object failed"}

      headers["x-amz-copy-source"] ->
        {:ok, %{}}

      String.starts_with?(path, "#{incoming_prefix}cubic/dmap/sample/") ->
        {:ok, %{}}

      true ->
        {:error, "put_object failed"}
    end
  end

  def request(%{service: :s3, http_method: :get, params: params, path: path}, _config_overrides)
      when params == %{} do
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    cond do
      path == "#{incoming_prefix}cubic/ods_qlik/SAMPLE/LOAD1.dfm" ->
        {:ok,
         %{
           body: """
            {
                "dataInfo": {
                    "columns": [
                        {
                            "name": "SAMPLE_ID"
                        },
                        {
                            "name": "SAMPLE_NAME"
                        },
                        {
                            "name": "EDW_INSERTED_DTM"
                        },
                        {
                            "name": "EDW_UPDATED_DTM"
                        }
                    ]
                }
            }
           """
         }}

      path == "#{incoming_prefix}cubic/ods_qlik/SAMPLE__ct/20220102-204950123.dfm" ->
        {:ok,
         %{
           body: """
            {
                "dataInfo": {
                    "columns": [
                        {
                            "name": "header__change_seq"
                        },
                        {
                            "name": "header__change_oper"
                        },
                        {
                            "name": "header__timestamp"
                        },
                        {
                            "name": "SAMPLE_ID"
                        },
                        {
                            "name": "SAMPLE_NAME"
                        },
                        {
                            "name": "EDW_INSERTED_DTM"
                        },
                        {
                            "name": "EDW_UPDATED_DTM"
                        }
                    ]
                }
            }
           """
         }}

      path == "#{incoming_prefix}cubic/ods_qlik/SAMPLE/invalid_LOAD2.dfm" ->
        {:ok,
         %{
           body: """
            {
                "dataInfo": {
                    "columns": [
                        {
                            "name": "SAMPLE_ID"
                        },
                        {
                            "name": "SAMPLE_NAME"
                        },
                        {
                            "name": "EDW_INSERTED_DTM"
                        },
                        {
                            "name": "EDW_UPDATED_DTM"
                        },
                        {
                            "name": "INVALID_COLUMN"
                        }
                    ]
                }
            }
           """
         }}

      path == "#{incoming_prefix}cubic/ods_qlik/SAMPLE/notfound_LOAD3.dfm" ->
        {:error,
         {:http_error, 404,
          %{
            body:
              "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>cubic/ods_qlik/SAMPLE/notfound_LOAD3.dfm</Key><RequestId>...</RequestId><HostId>...</HostId></Error>",
            headers: [],
            status_code: 404
          }}}

      true ->
        {:error, "get_object failed"}
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
    cubic_ods_qlik_sample_ct = cubic_ods_qlik <> "SAMPLE__ct/"
    cubic_dmap = cubic <> "dmap/"
    cubic_dmap_sample = cubic_dmap <> "sample/"

    case params do
      %{"prefix" => ^cubic_ods_qlik, "delimiter" => "/"} ->
        {:ok,
         %{
           body: %{
             common_prefixes: [
               %{prefix: cubic_ods_qlik_sample},
               %{prefix: cubic_ods_qlik_sample_ct}
             ],
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

      %{"prefix" => ^cubic_ods_qlik_sample_ct} ->
        {:ok,
         %{
           body: %{
             common_prefixes: [],
             contents: MockExAws.Data.load_objects(cubic_ods_qlik_sample_ct),
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

    valid_paths =
      Enum.map(MockExAws.Data.load_objects(), fn load_object ->
        "#{load_object[:key]}"
      end) ++
        [
          "cubic/dmap/sample/already_copied_but_source_not_deleted.csv.gz",
          "cubic/dmap/sample/timestamp=20220101T000000Z/already_copied_but_source_not_deleted.csv.gz",
          "cubic/dmap/sample/timestamp=20220101T000000Z/already_copied.csv.gz",
          "cubic/dmap/sample/move.csv.gz",
          "cubic/dmap/sample/move_with_copy_failing.csv.gz",
          "#{incoming_prefix}cubic/ods_qlik/SAMPLE/source_metadata_does_not_exist.csv.gz"
        ]

    if Enum.member?(valid_paths, path) do
      {:ok, %{}}
    else
      {:error, "head_object failed"}
    end
  end

  def request(%{service: :glue, data: %{RunId: "success_run_id"}}, _config_overrides) do
    {:ok, %{"JobRun" => %{"JobRunState" => "SUCCEEDED"}}}
  end

  def request(%{service: :glue, data: %{RunId: "error_run_id"}}, _config_overrides) do
    {:ok, %{"JobRun" => %{"JobRunState" => "ERROR"}}}
  end

  def request(%{service: :glue} = op, _config_overrides) do
    glue_database_incoming = Application.fetch_env!(:ex_cubic_ingestion, :glue_database_incoming)

    cond do
      Enum.member?(op.headers, {"x-amz-target", "AWSGlue.GetJobRun"}) ->
        {:ok, %{"JobRun" => %{"JobRunState" => "SUCCEEDED"}}}

      Enum.member?(op.headers, {"x-amz-target", "AWSGlue.GetTable"}) and
          op.data == %{DatabaseName: glue_database_incoming, Name: "cubic_ods_qlik__sample"} ->
        {:ok,
         %{
           "Table" => %{
             "DatabaseName" => glue_database_incoming,
             "Name" => "cubic_ods_qlik__sample",
             "StorageDescriptor" => %{
               "Columns" => [
                 %{
                   "Name" => "sample_id"
                 },
                 %{
                   "Name" => "sample_name"
                 },
                 %{
                   "Name" => "edw_inserted_dtm"
                 },
                 %{
                   "Name" => "edw_updated_dtm"
                 }
               ]
             }
           }
         }}

      Enum.member?(op.headers, {"x-amz-target", "AWSGlue.GetTable"}) and
          op.data == %{DatabaseName: glue_database_incoming, Name: "cubic_ods_qlik__sample__ct"} ->
        {:ok,
         %{
           "Table" => %{
             "DatabaseName" => glue_database_incoming,
             "Name" => "cubic_ods_qlik__sample",
             "StorageDescriptor" => %{
               "Columns" => [
                 %{
                   "Name" => "header__change_seq"
                 },
                 %{
                   "Name" => "header__change_oper"
                 },
                 %{
                   "Name" => "header__timestamp"
                 },
                 %{
                   "Name" => "sample_id"
                 },
                 %{
                   "Name" => "sample_name"
                 },
                 %{
                   "Name" => "edw_inserted_dtm"
                 },
                 %{
                   "Name" => "edw_updated_dtm"
                 }
               ]
             }
           }
         }}

      Enum.member?(op.headers, {"x-amz-target", "AWSGlue.StartJobRun"}) ->
        {:ok, %{"JobRunId" => "abc123"}}

      true ->
        {:error, "glue failed"}
    end
  end

  def request(
        %{service: :athena, data: %{QueryExecutionIds: ["error_batch_get_query_execution"]}},
        _config_overrides
      ) do
    {:error, "AmazonAthena.BatchGetQueryExecution failed"}
  end

  def request(
        %{service: :athena, data: %{QueryExecutionIds: ["success_batch_get_query_execution"]}},
        _config_overrides
      ) do
    {:ok,
     %{
       "QueryExecutions" => [
         %{"Status" => %{"State" => "SUCCEEDED"}}
       ]
     }}
  end

  def request(%{service: :athena, headers: headers, data: data}, _config_overrides) do
    cond do
      Enum.member?(headers, {"x-amz-target", "AmazonAthena.StartQueryExecution"}) ->
        {:ok, %{"QueryExecutionId" => "success_query_id"}}

      Enum.member?(headers, {"x-amz-target", "AmazonAthena.BatchGetQueryExecution"}) ->
        {:ok,
         %{
           "QueryExecutions" =>
             Enum.map(data."QueryExecutionIds", fn _query_execution_status ->
               %{"Status" => %{"State" => "SUCCEEDED"}}
             end)
         }}

      true ->
        {:error, "athena failed"}
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
