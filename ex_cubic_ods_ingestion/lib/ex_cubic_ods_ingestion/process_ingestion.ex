defmodule ExCubicOdsIngestion.ProcessIngestion do
  @moduledoc """
  ProcessIngestion module.
  """

  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  @spec archive([integer()]) :: [integer()]
  def archive(load_recs_ids) do
    CubicOdsLoad.update_many(load_recs_ids, status: "archived")
  end

  @spec error([integer()]) :: [integer()]
  def error(load_recs_ids) do
    CubicOdsLoad.update_many(load_recs_ids, status: "errored")
  end
end
