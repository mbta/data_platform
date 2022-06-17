defmodule ReleaseTasks.Application do
  @moduledoc """
  Module containing helper functions for dealing with the application.
  """

  @app :ex_cubic_ingestion

  @spec start :: :ok
  @doc """
  Starts application without the GenServers.
  """
  def start do
    # loads application configuration
    Application.load(@app)

    # disables running the full app and just start Oban and Ecto
    Application.put_env(@app, :start_app_children?, false)

    # starts app
    Application.ensure_all_started(@app)

    :ok
  end
end
