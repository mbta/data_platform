defmodule ExCubicIngestion.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    sys_children = [
      {ExCubicIngestion.Repo, []},
      {ExCubicIngestion.Repo.Migrator,
       run_migrations_at_startup?:
         Application.get_env(:ex_cubic_ingestion, :run_migrations_at_startup?)},
      {Oban, ReleaseTasks.ScheduleDmap.oban_config()}
    ]

    app_children =
      if Application.get_env(:ex_cubic_ingestion, :start_app_children?) do
        [
          {ExCubicIngestion.ProcessIncoming, []},
          {ExCubicIngestion.ValidateIncoming, []},
          {ExCubicIngestion.ProcessIngestion, []}
        ]
      else
        []
      end

    children = sys_children ++ app_children

    # attach telemetry to Oban, so exceptions get logged
    oban_events = [[:oban, :job, :start], [:oban, :job, :stop], [:oban, :job, :exception]]

    :ok =
      :telemetry.attach_many(
        "oban-handler",
        oban_events,
        &ExCubicIngestion.ObanLogger.handle_event/4,
        []
      )

    # attach a specific error handler for the Oban workers
    :ok =
      :telemetry.attach(
        "oban-worker-error",
        [:oban, :job, :exception],
        &ExCubicIngestion.ObanWorkerError.handle_event/4,
        []
      )

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: ExCubicIngestion.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
