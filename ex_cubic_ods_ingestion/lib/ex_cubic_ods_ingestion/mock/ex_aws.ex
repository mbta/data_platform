defmodule ExCubicOdsIngestion.Mock.ExAws do
  @moduledoc """
  ExCubicOdsIngestion.Mock.ExAws @todo
  """

  @spec request!(ExAws.Operation.t(), keyword) :: term
  def request!(op, _config_overrides \\ []) do
    op

    # ::::: original implementation :::::
    #
    # case request(op, config_overrides) do
    #   {:ok, result} ->
    #     result

    #   error ->
    #     raise ExAws.Error, """
    #     ExAws Request Error!
    #     #{inspect(error)}
    #     """
    # end
  end
end
