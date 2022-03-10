ExUnit.start()

# checkouts default to manual, `use ExCubicOdsIngestion.DataCase` for shared checkouts
Ecto.Adapters.SQL.Sandbox.mode(ExCubicOdsIngestion.Repo, :manual)
