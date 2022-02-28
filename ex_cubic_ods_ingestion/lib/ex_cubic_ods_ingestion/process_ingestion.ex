defmodule ExCubicOdsIngestion.ProcessIngestion do
  @moduledoc """
  ProcessIngestion module.
  """

  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  @spec archive(map()) :: [integer()]
  def archive(loads_chunk) do
    load_recs_ids = Enum.map(loads_chunk, fn %{"load" => load_rec} = _chunk -> load_rec["id"] end)
    CubicOdsLoad.update_many(load_recs_ids, status: "archived")
  end

  @spec error(map()) :: [integer()]
  def error(loads_chunk) do
    load_recs_ids = Enum.map(loads_chunk, fn %{"load" => load_rec} = _chunk -> load_rec["id"] end)
    CubicOdsLoad.update_many(load_recs_ids, status: "errored")
  end
end
