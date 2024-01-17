defmodule ExAws.Helpers do
  @moduledoc """
  Central place for useful functions that do ExAws work.
  """

  require Logger

  @doc """
  S3: Performs checks, before copying the source object to a destination object
  and deleting the source object (moving).
  """
  @spec move(module(), String.t(), String.t(), String.t(), String.t()) ::
          {:ok, term()} | {:error, term()}
  def move(lib_ex_aws, source_bucket, source_key, destination_bucket, destination_key) do
    {source_status, source_req_response} =
      lib_ex_aws.request(ExAws.S3.head_object(source_bucket, source_key))

    {destination_status, _req_response} =
      lib_ex_aws.request(ExAws.S3.head_object(destination_bucket, destination_key))

    cond do
      source_status == :error and destination_status == :ok ->
        {:ok, "Already copied."}

      source_status == :error ->
        {:error, source_req_response}

      destination_status == :ok ->
        lib_ex_aws.request(ExAws.S3.delete_object(source_bucket, source_key))

      # do the move
      true ->
        {copy_status, copy_req_response} =
          lib_ex_aws.request(
            ExAws.S3.put_object_copy(
              destination_bucket,
              destination_key,
              source_bucket,
              source_key
            )
          )

        if copy_status == :ok do
          lib_ex_aws.request(ExAws.S3.delete_object(source_bucket, source_key))
        else
          {:error, copy_req_response}
        end
    end
  end

  @doc """
  Athena: Do a batch call to get the status of all the query executions. Based on all the
  statuses, return :ok if all succeeded, and {:error, ...} otherwise.
  """
  @spec monitor_athena_query_executions(module(), [{:ok, map()}]) :: :ok | {:error, String.t()}
  def monitor_athena_query_executions(lib_ex_aws, success_requests) do
    {request_status, %{"QueryExecutions" => query_executions}} =
      success_requests
      |> Enum.map(fn {:ok, %{"QueryExecutionId" => query_execution_id}} ->
        query_execution_id
      end)
      |> get_athena_query_executions_status(lib_ex_aws)

    # return ok only if all the queries succeeded
    with :ok <- request_status,
         true <- Enum.all?(query_executions, &query_succeeded?/1) do
      Logger.info("Athena Query Executions Status: #{Jason.encode!(query_executions)}")

      :ok
    else
      _errored ->
        {:error, "Athena Batch Get Query Execution: #{Jason.encode!(query_executions)}"}
    end
  end

  @spec get_athena_query_executions_status([String.t()], module()) ::
          {:ok, map()} | {:error, String.t()}
  defp get_athena_query_executions_status(query_execution_ids, lib_ex_aws) do
    # pause a litte before getting status
    Process.sleep(2_000)

    batch_get_query_execution_request =
      query_execution_ids
      |> ExAws.Athena.batch_get_query_execution()
      |> lib_ex_aws.request()

    with {:ok, %{"QueryExecutions" => query_executions}} <- batch_get_query_execution_request,
         true <- queries_running?(query_executions) do
      Logger.info("Athena Query Executions Status: #{Jason.encode!(query_executions)}")

      get_athena_query_executions_status(query_execution_ids, lib_ex_aws)
    else
      _succeeded_or_errored ->
        batch_get_query_execution_request
    end
  end

  defp queries_running?(query_executions) do
    Enum.any?(query_executions, fn %{"Status" => %{"State" => state}} ->
      state == "QUEUED" || state == "RUNNING"
    end)
  end

  defp query_succeeded?(%{"Status" => %{"State" => "SUCCEEDED"}}) do
    true
  end

  defp query_succeeded?(%{
         "Status" => %{
           "State" => "FAILED",
           "AthenaError" => %{"ErrorMessage" => "Partition already exists."}
         }
       }) do
    true
  end

  defp query_succeeded?(%{
         "Status" => %{
           "State" => "FAILED",
           "AthenaError" => %{"ErrorMessage" => "Partition entries already exist."}
         }
       }) do
    true
  end

  defp query_succeeded?(_query_execution_status) do
    false
  end
end
