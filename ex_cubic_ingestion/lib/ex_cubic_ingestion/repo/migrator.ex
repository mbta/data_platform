defmodule ExCubicIngestion.Repo.Migrator do
  @moduledoc """
  GenServer which runs on startup to run Ecto migrations, then terminates.
  """

  use GenServer, restart: :temporary

  require Logger

  @opts [module: Ecto.Migrator, run_migrations_at_startup?: true]

  @spec start_link(Keyword.t()) :: :ignore
  def start_link(opts) do
    opts = Keyword.merge(@opts, opts)

    if Keyword.get(opts, :run_migrations_at_startup?) do
      Logger.info("Starting migrations (synchronous).")
      run!(opts[:module])
      Logger.info("Finished migrations.")
    end

    :ignore
  end

  # callbacks
  @impl GenServer
  def init(opts) do
    {:ok, opts}
  end

  # server functions
  @spec run!(module) :: :ok
  defp run!(module) do
    for repo <- Application.get_env(:ex_cubic_ingestion, :ecto_repos, []) do
      Logger.info(fn -> "Migrating repo=#{repo}" end)

      {time_usec, {:ok, _, _}} =
        :timer.tc(module, :with_repo, [repo, &module.run(&1, :up, all: true)])

      time_msec = System.convert_time_unit(time_usec, :microsecond, :millisecond)
      Logger.info("Migration finished repo=#{repo} time=#{time_msec}")
    end

    :ok
  end
end
