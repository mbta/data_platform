defmodule ExCubicOdsIngestion.ProcessIngestion do
  @moduledoc """
  ProcessIngestion module.
  """

  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  @spec error(CubicOdsLoad.t()) :: nil
  def archive(load_rec) do
    CubicOdsLoad.update_status(load_rec, "archive")
  end

  @spec error(CubicOdsLoad.t()) :: nil
  def error(load_rec) do
    CubicOdsLoad.update_status(load_rec, "error")
  end
end
