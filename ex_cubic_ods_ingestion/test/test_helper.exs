ExUnit.start()

# Set the pool mode to manual for explicit checkouts
Ecto.Adapters.SQL.Sandbox.mode(ExCubicOdsIngestion.Repo, {:shared, self()})
