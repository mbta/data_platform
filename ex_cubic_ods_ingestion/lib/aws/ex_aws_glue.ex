defmodule ExAws.Glue do
  @moduledoc """
  ExAws.Glue module.
  """

  @spec start_job_run(String.t(), map()) :: ExAws.Operation.t()
  def start_job_run(job_name, job_arguments) do
    %ExAws.Operation.JSON{
      http_method: :post,
      path: "/",
      headers: [
        {"x-amz-target", "AWSGlue.StartJobRun"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      data: %{
        JobName: job_name,
        Arguments: job_arguments
      },
      service: :glue
    }
  end

  @spec get_job_run(String.t(), String.t()) :: ExAws.Operation.t()
  def get_job_run(job_name, job_run_id) do
    %ExAws.Operation.JSON{
      http_method: :post,
      path: "/",
      headers: [
        {"x-amz-target", "AWSGlue.GetJobRun"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      data: %{
        JobName: job_name,
        RunId: job_run_id
      },
      service: :glue
    }
  end
end
