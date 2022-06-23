defmodule ReleaseTasks.RetryArchiveError do
  @moduledoc """
  This release task will retry archive and error jobs for loads that
  have failed to properly get to the 'archived' and 'errored' status.

  Based on: https://hexdocs.pm/phoenix/releases.html#ecto-migrations-and-custom-commands
  """

  alias ExCubicIngestion.Repo
  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Workers.Archive
  alias ExCubicIngestion.Workers.Error

  @app :ex_cubic_ingestion

  @spec run :: :ok
  def run do
    start_app()

    # get all loads that are stuck in 'archiving' and 'erroring'
    {archiving_loads, erroring_loads} =
      Enum.split_with(
        CubicLoad.all_by_status_in([
          "archiving",
          "archived_unknown",
          "erroring",
          "errored_unknown"
        ]),
        &(&1.status == "archiving" || &1.status == "archived_unknown")
      )

    Repo.transaction(fn ->
      Enum.each(archiving_loads, &Oban.insert!(Archive.new(%{load_rec_id: &1.id})))

      Enum.each(erroring_loads, &Oban.insert!(Error.new(%{load_rec_id: &1.id})))
    end)

    :ok
  end

  defp start_app do
    # loads application configuration
    Application.load(@app)

    # disables running the full app and just start Oban and Ecto
    Application.put_env(@app, :start_app_children?, false)

    # starts app
    Application.ensure_all_started(@app)
  end
end
