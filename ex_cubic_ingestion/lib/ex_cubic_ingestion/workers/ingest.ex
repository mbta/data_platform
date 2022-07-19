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

    # start glue job
    glue_job_start_request =
      load_rec_ids
      |> construct_glue_job_payload()
      |> start_glue_job_run(lib_ex_aws)

    case glue_job_start_request do
      {:ok, %{"JobRunId" => glue_job_run_id}} ->
        monitor_glue_job_run(glue_job_run_id, load_rec_ids, lib_ex_aws)

      _glue_job_start_request_error ->
        handle_start_glue_job_error(glue_job_start_request)
    end
  end

  @spec start_glue_job_run({String.t(), String.t()}, module()) :: {:ok, map()} | {:error, term()}
  defp start_glue_job_run({env_payload, input_payload}, lib_ex_aws) do
    bucket_operations = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_operations)

    prefix_operations = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_operations)

    glue_job_name =
      Application.fetch_env!(:ex_cubic_ingestion, :glue_job_cubic_ingestion_ingest_incoming)

    lib_ex_aws.request(
      ExAws.Glue.start_job_run(glue_job_name, %{
        "--extra-py-files":
          "s3://#{bucket_operations}/#{prefix_operations}packages/py_cubic_ingestion.zip",
        "--ENV": env_payload,
        "--INPUT": input_payload
      })
    )
  end

  @spec get_glue_job_run_status(module(), String.t()) :: map()
  defp get_glue_job_run_status(lib_ex_aws, run_id) do
    glue_job_name =
      Application.fetch_env!(:ex_cubic_ingestion, :glue_job_cubic_ingestion_ingest_incoming)

    # pause a litte before getting status
    Process.sleep(5000)

    glue_job_run_status =
      case lib_ex_aws.request(ExAws.Glue.get_job_run(glue_job_name, run_id)) do
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

      _other_glue_job_run_state ->
        glue_job_run_status
    end
  end

  @doc """
  Gets loads map by a list of IDs, and adds the ODS snapshot partition column if an ODS load.
  Otherwise it just uses the load map as is.
  """
  @spec construct_glue_job_payload([integer()]) :: {String.t(), String.t()}
  def construct_glue_job_payload(load_rec_ids) do
    glue_database_incoming = Application.fetch_env!(:ex_cubic_ingestion, :glue_database_incoming)

    glue_database_springboard =
      Application.fetch_env!(:ex_cubic_ingestion, :glue_database_springboard)

    bucket_incoming = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_incoming)
    bucket_springboard = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_springboard)

    prefix_incoming = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    prefix_springboard =
      Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_springboard)

    loads =
      Enum.map(CubicLoad.get_many_with_table(load_rec_ids), fn {load_rec, table_rec} ->
        %{
          id: load_rec.id,
          s3_key: load_rec.s3_key,
          table_name: table_rec.name,
          is_raw: load_rec.is_raw,
          partition_columns: [
            %{"name" => "identifier", "value" => Path.basename(load_rec.s3_key)}
          ]
        }
      end)

    # for loads that are from ODS, attach the snapshot partition
    loads_with_ods_snapshot = Enum.map(loads, &attach_ods_snapshot(&1))

    {Jason.encode!(%{
       GLUE_DATABASE_INCOMING: glue_database_incoming,
       GLUE_DATABASE_SPRINGBOARD: glue_database_springboard,
       S3_BUCKET_INCOMING: bucket_incoming,
       S3_BUCKET_PREFIX_INCOMING: prefix_incoming,
       S3_BUCKET_SPRINGBOARD: bucket_springboard,
       S3_BUCKET_PREFIX_SPRINGBOARD: prefix_springboard
     }),
     Jason.encode!(%{
       loads: loads_with_ods_snapshot
     })}
  end

  @spec attach_ods_snapshot(map()) :: map()
  defp attach_ods_snapshot(%{partition_columns: partition_columns} = load) do
    if String.starts_with?(load[:s3_key], "cubic/ods_qlik/") do
      ods_load_rec = CubicOdsLoadSnapshot.get_by!(load_id: load[:id])

      # note: order of partitions is intentional
      %{
        load
        | partition_columns: [
            %{
              "name" => "snapshot",
              "value" => Calendar.strftime(ods_load_rec.snapshot, "%Y%m%dT%H%M%SZ")
            }
            | partition_columns
          ]
      }
    else
      load
    end
  end

  @doc """
  Given a run ID, check status of it continously until it stops running. If its last
  status is a success, update loads' status to archive, and let the worker know the job
  was a success. Any other state should be considered an error for the job.
  """
  @spec monitor_glue_job_run(String.t(), [integer()], module()) :: Oban.Worker.result()
  def monitor_glue_job_run(glue_job_run_id, load_rec_ids, lib_ex_aws) do
    Logger.info("#{@log_prefix} Glue Job Run ID: #{glue_job_run_id}")

    # monitor for success or failure state in glue job run
    glue_job_run_status = get_glue_job_run_status(lib_ex_aws, glue_job_run_id)

    case glue_job_run_status do
      %{"JobRun" => %{"JobRunState" => "SUCCEEDED"}} ->
        Logger.info("#{@log_prefix} Glue Job Run Status: #{Jason.encode!(glue_job_run_status)}")

        CubicLoad.update_many(load_rec_ids, status: "ready_for_archiving")

        :ok

      _other_glue_job_run_state ->
        # note: error will be handled within ObanWorkerError module
        {:error, "Glue Job Run Status: #{Jason.encode!(glue_job_run_status)}"}
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
