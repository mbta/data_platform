defmodule ExCubicOdsIngestion.Repo.Migrator do
  @moduledoc """
  GenServer which runs on startup to run Ecto migrations, then terminates.
  """
  require Logger

  @opts [module: Ecto.Migrator, run_migrations_at_startup?: true]

  @spec start_link(Keyword.t()) :: :ignore
  def start_link(opts) do
    opts = Keyword.merge(@opts, opts)

    if Keyword.get(opts, :run_migrations_at_startup?) do
      Logger.info("Migrating synchronously")
      migrate!(opts[:module])
      _ignored = Logger.info("Finished migrations")
    end

    :ignore
  end

  @spec child_spec(Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient
    }
  end

  defp migrate!(module) do
    for repo <- [ExCubicOdsIngestion.Repo] do
      _ignored = Logger.info(fn -> "Migrating repo=#{repo}" end)

      {time_usec, {:ok, _, _}} =
        :timer.tc(module, :with_repo, [repo, &module.run(&1, :up, all: true)])

      time_msec = System.convert_time_unit(time_usec, :microsecond, :millisecond)
      Logger.info("Migration finished repo=#{repo} time=#{time_msec}")
    end

    :ok
  end

end
