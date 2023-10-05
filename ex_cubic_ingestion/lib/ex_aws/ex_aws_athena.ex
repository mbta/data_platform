defmodule ExAws.Athena do
  @moduledoc """
  ExAws.Athena module for making Athena requests.
  See https://github.com/aws/aws-sdk-go/blob/main/models/apis/athena/2017-05-18/api-2.json
  for constructing further requests.
  """

  require Ecto

  @doc """
  Build operation for 'start_query_execution' API
  """
  @spec start_query_execution(String.t(), map()) :: ExAws.Operation.t()
  def start_query_execution(query_string, result_configuration) do
    %ExAws.Operation.JSON{
      http_method: :post,
      path: "/",
      headers: [
        {"x-amz-target", "AmazonAthena.StartQueryExecution"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      data: %{
        ClientRequestToken: Ecto.UUID.generate(),
        QueryString: query_string,
        ResultConfiguration: result_configuration,
        WorkGroup: Application.fetch_env!(:ex_cubic_ingestion, :athena_workgroup)
      },
      service: :athena
    }
  end

  @doc """
  Build operation for 'batch_get_query_execution' API
  """
  @spec batch_get_query_execution([String.t()]) :: ExAws.Operation.t()
  def batch_get_query_execution(query_execution_ids) do
    %ExAws.Operation.JSON{
      http_method: :post,
      path: "/",
      headers: [
        {"x-amz-target", "AmazonAthena.BatchGetQueryExecution"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      data: %{
        QueryExecutionIds: query_execution_ids
      },
      service: :athena
    }
  end
end
