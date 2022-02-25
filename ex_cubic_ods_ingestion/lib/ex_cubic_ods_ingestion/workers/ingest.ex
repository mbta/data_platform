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
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  require Logger

  @job_timeout_in_sec 300

  @impl Oban.Worker
  def perform(%{args: args, meta: meta} = job) do
    # if no glue job run id available from meta data, then start the job
    glue_job_run_id = if is_nil(Map.get(meta, "glue_job_run_id")) do
      {:ok, glue_job_run} = start_glue_job_run("{\"a\": \"1\",\"b\": \"2\"}", "{\"c\": \"3\",\"d\": \"4\"}")

      job
      |> Changeset.change(meta: Map.put(meta, "glue_job_run_id", glue_job_run["JobRunId"]))
      |> Repo.update()

      glue_job_run["JobRunId"]
    else
      Map.get(meta, "glue_job_run_id")
    end

    # monitor for success or failure state in glue job run
    glue_job_run_status = get_glue_job_run_status(glue_job_run_id)

    case glue_job_run_status do
      "SUCCEEDED" ->
        ProcessIngestion.archive(Map.fetch!(args, "load_id"))
      _ ->
        ProcessIngestion.error(Map.fetch!(args, "load_id"))
    end

    :ok
  end

  @impl Oban.Worker
  def timeout(_job), do: :timer.seconds(@job_timeout_in_sec)

  @spec start_glue_job_run(String.t(), String.t()) :: {atom(), map()}
  def start_glue_job_run(env_arg, input_arg) do
    operation = %ExAws.Operation.JSON{
      http_method: :post,
      path: "/",
      headers: [
        {"x-amz-target", "AWSGlue.StartJobRun"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      data: %{
        "JobName": "vehiclepositions-2021-09-06",
        "Arguments": %{
          "--ENV": env_arg,
          "--INPUT": input_arg
        }
      },
      service: :glue
    }

    ExAws.request(operation)
  end

  @spec get_glue_job_run_status(String.t()) :: {atom(), map()}
  def get_glue_job_run_status(run_id) do
    Process.sleep(2000)

    operation = %ExAws.Operation.JSON{
      http_method: :post,
      path: "/",
      headers: [
        {"x-amz-target", "AWSGlue.GetJobRun"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      data: %{
        JobName: "vehiclepositions-2021-09-06",
        RunId: run_id
      },
      service: :glue
    }

    {:ok, glue_job_run} = ExAws.request(operation)

    if glue_job_run["JobRun"]["JobRunState"] == "RUNNING" do
      get_glue_job_run_status(run_id)
    else
      glue_job_run["JobRun"]["JobRunState"]
    end

  end
end
