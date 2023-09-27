defmodule ExAws.Glue do
  @moduledoc """
  ExAws.Glue module for making Glue requests.
  See https://github.com/aws/aws-sdk-go/blob/main/models/apis/glue/2017-03-31/api-2.json
  for constructing further requests.
  """

  @spec start_job_run(String.t(), map()) :: ExAws.Operation.t()
  def start_job_run(job_name, arguments) do
    %ExAws.Operation.JSON{
      http_method: :post,
      path: "/",
      headers: [
        {"x-amz-target", "AWSGlue.StartJobRun"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      data: %{
        JobName: job_name,
        Arguments: arguments
      },
      service: :glue
    }
  end

  @spec get_job_run(String.t(), String.t()) :: ExAws.Operation.t()
  def get_job_run(job_name, run_id) do
    %ExAws.Operation.JSON{
      http_method: :post,
      path: "/",
      headers: [
        {"x-amz-target", "AWSGlue.GetJobRun"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      data: %{
        JobName: job_name,
        RunId: run_id
      },
      service: :glue
    }
  end

  @spec get_table(String.t(), String.t()) :: ExAws.Operation.t()
  def get_table(database_name, name) do
    %ExAws.Operation.JSON{
      http_method: :post,
      path: "/",
      headers: [
        {"x-amz-target", "AWSGlue.GetTable"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      data: %{
        DatabaseName: database_name,
        Name: name
      },
      service: :glue
    }
  end
end
