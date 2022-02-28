defmodule ExCubicOdsIngestion.Workers.Ingest do
  @moduledoc """
  Workers.Ingest module.
  """

  use Oban.Worker,
    queue: :ingest,
    max_attempts: 3

  alias Ecto.Changeset
  alias ExCubicOdsIngestion.ProcessIngestion
  alias ExCubicOdsIngestion.Repo

  @impl Oban.Worker
  def perform(%{args: args, meta: meta} = job) do
    # get info passed into args
    %{"chunk" => loads_chunk} = args

    # allow for ex_aws module to be passed in as a string, since Oban will need to
    # serialize args to JSON. defaulted to library module.
    lib_ex_aws =
      case args do
        %{"lib_ex_aws" => mod_str} -> Module.safe_concat([mod_str])
        _args_lib_ex_aws -> ExAws
      end

    # if no glue job run id available from meta data, then start the job
    glue_job_run_id =
      if is_nil(Map.get(meta, "glue_job_run_id")) do
        {:ok, glue_job_run} =
          start_glue_job_run(
            lib_ex_aws,
            "{\"a\": \"1\",\"b\": \"2\"}",
            "{\"c\": \"3\",\"d\": \"4\"}"
          )

        # update job's meta data with glue job run id
        job
        |> Changeset.change(meta: %{"glue_job_run_id" => glue_job_run["JobRunId"]})
        |> Repo.update()

        glue_job_run["JobRunId"]
      else
        Map.get(meta, "glue_job_run_id")
      end

    # monitor for success or failure state in glue job run
    case get_glue_job_run_status(lib_ex_aws, glue_job_run_id) do
      {:ok, %{"JobRun" => %{"JobRunState" => "SUCCEEDED"}}} ->
        ProcessIngestion.archive(loads_chunk)

      _other_glue_job_run_state ->
        ProcessIngestion.error(loads_chunk)
    end

    :ok
  end

  @spec start_glue_job_run(any(), String.t(), String.t()) :: {atom(), map()}
  defp start_glue_job_run(lib_ex_aws, env_arg, input_arg) do
    glue_job_name = Application.fetch_env!(:ex_cubic_ods_ingestion, :glue_job_cubic_ods_ingest)

    operation = %ExAws.Operation.JSON{
      http_method: :post,
      path: "/",
      headers: [
        {"x-amz-target", "AWSGlue.StartJobRun"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      data: %{
        JobName: glue_job_name,
        Arguments: %{
          "--ENV": env_arg,
          "--INPUT": input_arg
        }
      },
      service: :glue
    }

    lib_ex_aws.request(operation)
  end

  @spec get_glue_job_run_status(any(), String.t()) :: {atom(), map()}
  defp get_glue_job_run_status(lib_ex_aws, run_id) do
    glue_job_name = Application.fetch_env!(:ex_cubic_ods_ingestion, :glue_job_cubic_ods_ingest)

    operation = %ExAws.Operation.JSON{
      http_method: :post,
      path: "/",
      headers: [
        {"x-amz-target", "AWSGlue.GetJobRun"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      data: %{
        JobName: glue_job_name,
        RunId: run_id
      },
      service: :glue
    }

    # pause a litte before getting status
    Process.sleep(5000)
    glue_job_run = lib_ex_aws.request(operation)

    case glue_job_run do
      {:ok, %{"JobRun" => %{"JobRunState" => "RUNNING"}}} ->
        get_glue_job_run_status(lib_ex_aws, run_id)

      _other_glue_job_run_state ->
        glue_job_run
    end
  end
end
