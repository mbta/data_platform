defmodule MockExAws do
  @moduledoc """
  MockExAws @todo
  """

  @spec request(ExAws.Operation.t(), keyword) :: term
  def request(op, config_overrides \\ [])

  def request(%{service: :s3} = op, _config_overrides) do
    incoming_prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_prefix_incoming)

    if op.params["prefix"] == "#{incoming_prefix}cubic_ods_qlik_test/" do
      {:ok,
       %{
         body: %{
           contents: MockExAws.Data.load_objects(),
           next_continuation_token: ""
         }
       }}
    else
      {:ok,
       %{
         body: %{
           contents: [],
           next_continuation_token: ""
         }
       }}
    end
  end

  def request(%{service: :glue} = op, _config_overrides) do
    cond do
      Enum.member?(op.headers, {"x-amz-target", "AWSGlue.StartJobRun"}) ->
        {:ok, %{"JobRunId" => "abc123"}}

      Enum.member?(op.headers, {"x-amz-target", "AWSGlue.GetJobRun"}) ->
        {:ok, %{"JobRun" => %{"JobRunState" => "SUCCEEDED"}}}

      true ->
        {:error, %{}}
    end
  end

  @spec request!(ExAws.Operation.t(), keyword) :: term
  def request!(op, config_overrides \\ []) do
    case request(op, config_overrides) do
      {:ok, result} ->
        result

      error ->
        raise ExAws.Error, """
        ExAws Request Error!
        #{inspect(error)}
        """
    end
  end
end
