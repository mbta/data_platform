defmodule ExCubicOdsIngestion.ProcessIngestion do
  @moduledoc """
  ProcessIngestion module.
  """

  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  @spec archive(CubicOdsLoad.t()) :: CubicOdsLoad.t()
  def archive(load_id) do
    load_rec = CubicOdsLoad.get!(load_id)
    CubicOdsLoad.update(load_rec, %{status: "archived"})
  end

  @spec error(CubicOdsLoad.t()) :: CubicOdsLoad.t()
  def error(load_id) do
    load_rec = CubicOdsLoad.get!(load_id)
    CubicOdsLoad.update(load_rec, %{status: "errored"})
  end
end
