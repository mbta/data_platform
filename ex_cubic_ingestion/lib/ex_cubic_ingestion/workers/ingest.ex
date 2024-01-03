defmodule ExCubicIngestion.Workers.Ingest do
  @moduledoc """
  Oban Worker that takes a list of load record IDs and runs a Glue job for all of them.
  The worker attaches itself to the Glue job by monitoring its status, only succeeding/failing
  when it succeeds/fails.
  """

  use Oban.Worker,
    queue: :ingest,
    max_attempts: 3

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot

  require Logger

  @log_prefix "[ex_cubic_ingestion] [workers] [ingest]"
  # 15 minutes
  @job_timeout_in_sec 900

  @impl Oban.Worker
  def timeout(_job), do: :timer.seconds(@job_timeout_in_sec)

  @impl Oban.Worker
  def perform(%{args: args} = _job) do
    # get list of ids for load records
    %{"load_rec_ids" => load_rec_ids} = args

    # allow for ex_aws module to be passed in as a string, since Oban will need to
    # serialize args to JSON. defaulted to library module.
    lib_ex_aws =
      case args do
        %{"lib_ex_aws" => mod_str} -> Module.safe_concat([mod_str])
        _args_lib_ex_aws -> ExAws
      end

    # gather the information needed to make AWS requests
    job_payload = construct_job_payload(load_rec_ids)

    with :ok <- run_glue_job(lib_ex_aws, job_payload),
         :ok <- add_athena_partitions(lib_ex_aws, job_payload) do
      update_statuses(job_payload)
    end
  end

  # Starts the glue job with the given payloads. If successful in starting, monitor the glue job
  # by checking its status until succeeded or failed. If failed to start, handle the error based
  # on the type error. See handle_start_glue_job_error/1.
  @spec run_glue_job(module(), {map(), map()}) :: Oban.Worker.result()
  defp run_glue_job(lib_ex_aws, {env_payload, input_payload}) do
    case start_glue_job_run(lib_ex_aws, {env_payload, input_payload}) do
      {:ok, %{"JobRunId" => glue_job_run_id}} ->
        monitor_glue_job_run(lib_ex_aws, glue_job_run_id)

      {:error, _body} = glue_job_start_request ->
        handle_start_glue_job_error(glue_job_start_request)
    end
  end

  @spec start_glue_job_run(module(), {map(), map()}) :: {:ok, map()} | {:error, term()}
  defp start_glue_job_run(lib_ex_aws, {env_payload, input_payload}) do
    bucket_operations = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_operations)

    prefix_operations = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_operations)

    glue_job_name =
      Application.fetch_env!(:ex_cubic_ingestion, :glue_job_cubic_ingestion_ingest_incoming)

    lib_ex_aws.request(
      ExAws.Glue.start_job_run(glue_job_name, %{
        "--extra-py-files":
          "s3://#{bucket_operations}/#{prefix_operations}packages/py_cubic_ingestion.zip",
        "--ENV": Jason.encode!(env_payload),
        "--INPUT": Jason.encode!(input_payload)
      })
    )
  end

  @doc """
  Given a run ID, check status of it continously until it stops running. If its last
  status is a success, update loads' status to archive, and let the worker know the job
  was a success. Any other state should be considered an error for the job.
  """
  @spec monitor_glue_job_run(module(), String.t()) :: Oban.Worker.result()
  def monitor_glue_job_run(lib_ex_aws, glue_job_run_id) do
    Logger.info("#{@log_prefix} Glue Job Run ID: #{glue_job_run_id}")

    # monitor for success or failure state in glue job run
    glue_job_run_status = get_glue_job_run_status(lib_ex_aws, glue_job_run_id)

    case glue_job_run_status do
      %{"JobRun" => %{"JobRunState" => "SUCCEEDED"}} ->
        Logger.info("#{@log_prefix} Glue Job Run Status: #{Jason.encode!(glue_job_run_status)}")

        :ok

      _other_glue_job_run_state ->
        # note: error will be handled within ObanWorkerError module
        {:error, "Glue Job Run Status: #{Jason.encode!(glue_job_run_status)}"}
    end
  end

  # Checks the Glue job's run status. If it's still running, re-check in 30 seconds.
  @spec get_glue_job_run_status(module(), String.t()) :: map()
  defp get_glue_job_run_status(lib_ex_aws, run_id) do
    glue_job_name =
      Application.fetch_env!(:ex_cubic_ingestion, :glue_job_cubic_ingestion_ingest_incoming)

    # pause a litte before getting status
    Process.sleep(30_000)

    glue_job_run_status_request =
      glue_job_name
      |> ExAws.Glue.get_job_run(run_id)
      |> lib_ex_aws.request()

    glue_job_run_status =
      case glue_job_run_status_request do
        {:ok, response} ->
          response

        {:error, {"ThrottlingException", message}} ->
          # keep running and try again after waiting a bit
          %{
            "JobRun" => %{
              "JobRunState" => "RUNNING",
              "ExAws.Error" => "ThrottlingException: #{message}"
            }
          }

        {:error, {exception, message}} ->
          # @todo how should we handle these errors?
          %{
            "JobRun" => %{"JobRunState" => "RUNNING", "ExAws.Error" => "#{exception}: #{message}"}
          }
      end

    case glue_job_run_status do
      %{"JobRun" => %{"JobRunState" => "RUNNING"}} ->
        Logger.info("#{@log_prefix} Glue Job Run Status: #{Jason.encode!(glue_job_run_status)}")

        get_glue_job_run_status(lib_ex_aws, run_id)

      _glue_job_run_status ->
        glue_job_run_status
    end
  end

  # If Glue job is successful, adds the Athena partition for each load only by start a query
  # execution with the "ALTER TABLE" statement, and then doing a batched status call for all the
  # queries.
  @spec add_athena_partitions(module(), {map(), map()}) :: Oban.Worker.result()
  defp add_athena_partitions(lib_ex_aws, {_env_payload, %{loads: loads}}) do
    success_error_requests =
      loads
      # make requests to start query executions
      |> Enum.map(&start_add_partition_query_execution(lib_ex_aws, &1))
      # split into successful requests and failures
      |> Enum.split_with(fn {status, _response_body} ->
        status == :ok
      end)

    case success_error_requests do
      # if all succesful, monitor their status
      {success_requests, []} ->
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

  @spec start_add_partition_query_execution(module(), map()) :: {:ok, term()} | {:error, term()}
  defp start_add_partition_query_execution(lib_ex_aws, load) do
    bucket_operations = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_operations)

    prefix_operations = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_operations)

    partitions =
      Enum.map_join(load.partition_columns, ", ", fn partition_column ->
        "#{partition_column.name} = '#{partition_column.value}'"
      end)

    # sleep a little to avoid throttling, at most 10 requests will be made per job
    Process.sleep(1000)

    lib_ex_aws.request(
      ExAws.Athena.start_query_execution(
        "ALTER TABLE #{load.destination_table_name} ADD PARTITION (#{partitions});",
        %{OutputLocation: "s3://#{bucket_operations}/#{prefix_operations}athena/"}
      )
    )
  end

  # If adding the partition to Athena is successful, update the status of all loads
  # to 'ready_for_archiving' allowing the archiving process to begin.
  @spec update_statuses({map(), map()}) :: Oban.Worker.result()
  defp update_statuses({_env_payload, %{loads: loads}}) do
    loads
    |> Enum.map(&Map.fetch!(&1, :id))
    |> CubicLoad.update_many(status: "ready_for_archiving")

    :ok
  end

  @doc """
  Gets loads map by a list of IDs, and adds the ODS snapshot partition column if an ODS load.
  Otherwise it just uses the load map as is.
  """
  @spec construct_job_payload([integer()]) :: {map(), map()}
  def construct_job_payload(load_rec_ids) do
    glue_database_incoming = Application.fetch_env!(:ex_cubic_ingestion, :glue_database_incoming)

    glue_database_springboard =
      Application.fetch_env!(:ex_cubic_ingestion, :glue_database_springboard)

    loads = Enum.map(CubicLoad.get_many_with_table(load_rec_ids), &CubicLoad.glue_job_payload/1)

    # for loads that are from ODS, attach the snapshot partition
    loads_with_ods_snapshot = Enum.map(loads, &attach_ods_snapshot(&1))

    {%{
       GLUE_DATABASE_INCOMING: glue_database_incoming,
       GLUE_DATABASE_SPRINGBOARD: glue_database_springboard
     },
     %{
       loads: loads_with_ods_snapshot
     }}
  end

  @spec attach_ods_snapshot(map()) :: map()
  defp attach_ods_snapshot(%{partition_columns: partition_columns} = load) do
    if CubicLoad.ods_load?(load[:s3_key]) do
      ods_load_snapshot_rec = CubicOdsLoadSnapshot.get_latest_by!(load_id: load[:id])

      # note: order of partitions is intentional
      %{
        load
        | partition_columns: [
            %{
              name: "snapshot",
              value: CubicOdsLoadSnapshot.formatted(ods_load_snapshot_rec.snapshot)
            }
            | partition_columns
          ]
      }
    else
      load
    end
  end

  @doc """
  Not all failures are equal when starting a glue job. For when we have exceeded the max
  concurrency, we just want to snooze the Oban job, so we can retry it again. Same thing
  for any throttling errors. If any other error occurs, we will also error out the Oban job.
  """
  @spec handle_start_glue_job_error({:error, term()}) :: Oban.Worker.result()
  def handle_start_glue_job_error({:error, {"ConcurrentRunsExceededException", message}}) do
    Logger.info(
      "#{@log_prefix} Glue Job Start Request: ConcurrentRunsExceededException: #{message}"
    )

    {:snooze, 60}
  end

  def handle_start_glue_job_error({:error, {"ThrottlingException", message}}) do
    Logger.info("#{@log_prefix} Glue Job Start Request: ThrottlingException: #{message}")

    {:snooze, 60}
  end

  def handle_start_glue_job_error({:error, {exception, message}}) do
    Logger.error("#{@log_prefix} Glue Job Start Request: #{exception}: #{message}")

    {:error, "Glue Job Start Request: #{exception}: #{exception}"}
  end
end
