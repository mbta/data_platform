defmodule ReleaseTasks.Utilities do
  @moduledoc """
  Module containing functions with commonly-used functionality.
  """

  @app :ex_cubic_ingestion

  @spec start_app :: :ok
  @doc """
  Starts 'ex_cubic_ingestion' application without the GenServers.
  """
  def start_app do
    # loads application configuration
    Application.load(@app)

    # disables running the full app and just start Oban and Ecto
    Application.put_env(@app, :start_app_children?, false)

    # starts app
    Application.ensure_all_started(@app)

    :ok
  end
end
