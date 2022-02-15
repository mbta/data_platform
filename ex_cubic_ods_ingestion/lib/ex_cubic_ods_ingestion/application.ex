defmodule ExCubicOdsIngestion.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    db_children = [
      {ExCubicOdsIngestion.Repo, []},
      {ExCubicOdsIngestion.Repo.Migrator,
       run_migrations_at_startup?:
         Application.get_env(:ex_cubic_ods_ingestion, :run_migrations_at_startup?)}
    ]

    app_children =
      if Application.get_env(:ex_cubic_ods_ingestion, :start_app?) do
        [
          {ExCubicOdsIngestion.ProcessIncoming, []}
        ]
      else
        []
      end

    children = db_children ++ app_children

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: ExCubicOdsIngestion.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
