defmodule ExCubicIngestion.Workers.LoadPartitions do
  @moduledoc """
  This worker will run every hour to refresh partitions for Athena
  for a few select tables.
  """

  use Oban.Worker,
    queue: :load_partitions,
    max_attempts: 1

  require Logger

  @impl Oban.Worker
  def perform(%{args: args} = _job) do
    %{"table_names" => table_names} = args

    # allow for ex_aws module to be passed in as a string, since Oban will need to
    # serialize args to JSON. defaulted to library module.
    lib_ex_aws =
      case args do
        %{"lib_ex_aws" => mod_str} -> Module.safe_concat([mod_str])
        _args_lib_ex_aws -> ExAws
      end

    case start_load_partitions(lib_ex_aws, table_names) do
      # if all succesful, monitor their status
      {[_first | _rest] = success_requests, []} ->
        ExAws.Helpers.monitor_athena_query_executions(lib_ex_aws, success_requests)

      # if any failures, fail the job as well
      {_success_requests, error_requests} ->
        error_requests_bodies =
          error_requests
          |> Enum.map(fn {:error, {exception, message}} ->
            %{exception: exception, message: message}
          end)
          |> Jason.encode!()

        {:error, "Athena Start Query Executions: #{error_requests_bodies}"}
    end
  end

  @spec start_load_partitions(module(), [String.t()]) :: {[{:ok, map()}], [{:error, map()}]}
  def start_load_partitions(lib_ex_aws, table_names) do
    bucket_operations = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_operations)
    prefix_operations = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_operations)

    table_names
    # make requests to start loading partitions
    |> Enum.map(fn table ->
      # sleep a little to avoid throttling
      Process.sleep(100)

      "MSCK REPAIR TABLE #{table};"
      |> ExAws.Athena.start_query_execution(%{
        OutputLocation: "s3://#{bucket_operations}/#{prefix_operations}athena/"
      })
      |> lib_ex_aws.request()
    end)
    # split into successful requests and failures
    |> Enum.split_with(fn {status, _response_body} ->
      status == :ok
    end)
  end
end
