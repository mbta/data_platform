defmodule ExCubicOdsIngestion.Repo.Migrator do
  @moduledoc """
  GenServer which runs on startup to run Ecto migrations, then terminates.
  """

  use GenServer, restart: :transient

  require Logger

  @opts [module: Ecto.Migrator]

  @spec start_link(Keyword.t()) :: :ignore
  def start_link(opts) do
    opts = Keyword.merge(@opts, opts)

    Logger.info("Starting migrations (synchronous).")
    run!(opts[:module])
    Logger.info("Finished migrations.")

    :ignore
  end

  # callbacks
  @impl true
  def init(opts) do
    {:ok, opts}
  end

  # server functions
  defp run!(module) do
    for repo <- [ExCubicOdsIngestion.Repo] do
      Logger.info(fn -> "Migrating repo=#{repo}" end)

      {time_usec, {:ok, _, _}} =
        :timer.tc(module, :with_repo, [repo, &module.run(&1, :up, all: true)])

      time_msec = System.convert_time_unit(time_usec, :microsecond, :millisecond)
      Logger.info("Migration finished repo=#{repo} time=#{time_msec}")
    end

    :ok
  end
end
