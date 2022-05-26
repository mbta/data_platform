defmodule ExCubicIngestion.Workers.ScheduleDmap do
  @moduledoc """
  @todo
  """

  use Oban.Worker,
    queue: :fetch_dmap,
    max_attempts: 1

  @impl Oban.Worker
  def perform(%{args: _args} = _job) do
    # IO.inspect(" :::::: HERE :::::")

    :ok
  end
end
