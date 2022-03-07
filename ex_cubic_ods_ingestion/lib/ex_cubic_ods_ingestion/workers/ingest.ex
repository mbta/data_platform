defmodule ExCubicOdsIngestion.Workers.Ingest do
  @moduledoc """
  Workers.Ingest module.
  """

  use Oban.Worker,
    queue: :ingest,
    max_attempts: 3

  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  require Logger

  @log_prefix "[ex_cubic_ods_ingestion][workers][ingest]"
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
    glue_database = Application.fetch_env!(:ex_cubic_ods_ingestion, :glue_database)

    %{"JobRunId" => glue_job_run_id} =
      start_glue_job_run(
        lib_ex_aws,
        "{\"GLUE_DATABASE_NAME\": \"#{glue_database}\"}",
        "{\"c\": \"3\",\"d\": \"4\"}"
      )

    Logger.info("#{@log_prefix} Glue Job Run ID: #{glue_job_run_id}")

    # monitor for success or failure state in glue job run
    glue_job_run_status = get_glue_job_run_status(lib_ex_aws, glue_job_run_id)

    case glue_job_run_status do
      %{"JobRun" => %{"JobRunState" => "SUCCEEDED"}} ->
        Logger.info("#{@log_prefix} Glue Job Run Status: #{Jason.encode!(glue_job_run_status)}")

        CubicOdsLoad.update_many(load_rec_ids, status: "ready_for_archiving")

      _other_glue_job_run_state ->
        Logger.error("#{@log_prefix} Glue Job Run Status: #{Jason.encode!(glue_job_run_status)}")

        CubicOdsLoad.update_many(load_rec_ids, status: "ready_for_erroring")
    end

    :ok
  end

  @spec start_glue_job_run(module(), String.t(), String.t()) :: map()
  defp start_glue_job_run(lib_ex_aws, env_arg, input_arg) do
    glue_job_name = Application.fetch_env!(:ex_cubic_ods_ingestion, :glue_job_cubic_ods_ingest)

    lib_ex_aws.request!(
      ExAws.Glue.start_job_run(glue_job_name, %{
        "--ENV": env_arg,
        "--INPUT": input_arg
      })
    )
  end

  @spec get_glue_job_run_status(module(), String.t()) :: map()
  defp get_glue_job_run_status(lib_ex_aws, run_id) do
    glue_job_name = Application.fetch_env!(:ex_cubic_ods_ingestion, :glue_job_cubic_ods_ingest)

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
end
