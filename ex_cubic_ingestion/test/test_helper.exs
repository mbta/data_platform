ExUnit.start()

# checkouts default to manual, `use ExCubicIngestion.DataCase` for shared checkouts
Ecto.Adapters.SQL.Sandbox.mode(ExCubicIngestion.Repo, :manual)
