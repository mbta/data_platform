defmodule ExCubicIngestion.Workers.Ingest do
  @moduledoc """
  Workers.Ingest module.
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
    {env_payload, input_payload} = construct_glue_job_payload(load_rec_ids)
    %{"JobRunId" => glue_job_run_id} = start_glue_job_run(lib_ex_aws, env_payload, input_payload)

    Logger.info("#{@log_prefix} Glue Job Run ID: #{glue_job_run_id}")

    # monitor for success or failure state in glue job run
    glue_job_run_status = get_glue_job_run_status(lib_ex_aws, glue_job_run_id)

    case glue_job_run_status do
      %{"JobRun" => %{"JobRunState" => "SUCCEEDED"}} ->
        Logger.info("#{@log_prefix} Glue Job Run Status: #{Jason.encode!(glue_job_run_status)}")

        CubicLoad.update_many(load_rec_ids, status: "ready_for_archiving")

        :ok

      _other_glue_job_run_state ->
        # note: error will be handled within ObanIngestWorkerError module
        {:error, "Glue Job Run Status: #{Jason.encode!(glue_job_run_status)}"}
    end
  end

  @spec start_glue_job_run(module(), String.t(), String.t()) :: map()
  defp start_glue_job_run(lib_ex_aws, env_payload, input_payload) do
    bucket_operations = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_operations)

    prefix_operations = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_operations)

    glue_job_name =
      Application.fetch_env!(:ex_cubic_ingestion, :glue_job_cubic_ingestion_ingest_incoming)

    lib_ex_aws.request!(
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
    glue_job_run_status = lib_ex_aws.request!(ExAws.Glue.get_job_run(glue_job_name, run_id))

    case glue_job_run_status do
      %{"JobRun" => %{"JobRunState" => "RUNNING"}} ->
        Logger.info("#{@log_prefix} Glue Job Run Status: #{Jason.encode!(glue_job_run_status)}")

        get_glue_job_run_status(lib_ex_aws, run_id)

      _other_glue_job_run_state ->
        glue_job_run_status
    end
  end

  @spec construct_glue_job_payload([integer()]) :: {String.t(), String.t()}
  def construct_glue_job_payload(load_rec_ids) do
    glue_database_incoming = Application.fetch_env!(:ex_cubic_ingestion, :glue_database_incoming)

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
          table_name: table_rec.name
        }
      end)

    {ods_loads, generic_loads} =
      Enum.split_with(loads, &String.starts_with?(&1[:s3_key], "cubic/ods_qlik/"))

    # for ODS loads, update with the snapshot value
    ods_loads_with_snapshot =
      Enum.map(ods_loads, fn load ->
        ods_load_rec = CubicOdsLoadSnapshot.get_by!(load_id: load[:id])

        Map.put(load, :snapshot, Calendar.strftime(ods_load_rec.snapshot, "%Y%m%dT%H%M%SZ"))
      end)

    {Jason.encode!(%{
       GLUE_DATABASE_INCOMING: glue_database_incoming,
       S3_BUCKET_INCOMING: bucket_incoming,
       S3_BUCKET_PREFIX_INCOMING: prefix_incoming,
       S3_BUCKET_SPRINGBOARD: bucket_springboard,
       S3_BUCKET_PREFIX_SPRINGBOARD: prefix_springboard
     }),
     Jason.encode!(%{
       generic_loads: generic_loads,
       ods_loads: ods_loads_with_snapshot
     })}
  end
end
