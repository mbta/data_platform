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

  @spec run :: :ok
  @doc """
  Inserts an Archive/Error job for each load with the status "archiving",
  "archived_unknown", "erroring", and "errored_unknown".
  """
  def run do
    ReleaseTasks.Application.start()

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
end
